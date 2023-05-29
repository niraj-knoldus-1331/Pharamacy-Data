package com.knoldus.pharmacy.transformations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.knoldus.pharmacy.Application;
import com.knoldus.pharmacy.models.TableRowSpecs;
import com.knoldus.pharmacy.options.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

import static com.knoldus.pharmacy.Application.invalidRecords;
import static com.knoldus.pharmacy.Application.validRecords;
import static com.knoldus.pharmacy.schema.BQJsonToBQSchema.deadLetterQueueBQSchema;
import static com.knoldus.pharmacy.utils.BeamPipelineConstants.*;
import static com.knoldus.pharmacy.utils.Utility.extractDeadLetterEvent;


public class PubSubMessagetoTableRow extends PTransform<PCollection<String>, PCollectionTuple> {
    final Logger logger = LoggerFactory.getLogger(PubSubMessagetoTableRow.class);


    @Override
    public PCollectionTuple expand(PCollection<String> input) {
        return input.apply("MapToRecord", ParDo.of(new DoFn<String, KV<TableRow, TableRowSpecs>>() {

            @ProcessElement
            public void process(ProcessContext processContext, PipelineOptions options) {
                BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);
                UUID uuid = UUID.randomUUID();
                String bqDataset = bigQueryOptions.getBqDataset();
                String deadLetterQueue = bigQueryOptions.getDeadLetterQueue();
                String retryDeadLetterQueue = bigQueryOptions.getRetryDeadLetterQueue();
                ObjectMapper mapper = bigQueryOptions.getDefaultObjectMapper();
                TableRow validRow = new TableRow();
                Map<String, Object> flattenKeyMap = new HashMap<>();
                Map<String, Object> medicationForm = new HashMap<>();
                String message = processContext.element();
                try {
                    Storage gcsClient = bigQueryOptions.getGcsClient();
                    String schemaBucketName = bigQueryOptions.getSchemaBucket();
                    logger.info("Flattening the Json Message");
                    Map<String, Object> flattenJson = JsonFlattener.flattenAsMap(message);
                    logger.info(String.format("Flatten Json is {%s}", flattenJson));

                    flattenJson.keySet().forEach(key -> {
                        String newKey = key.replace("-", "_");
                        flattenKeyMap.put(newKey, flattenJson.get(key));
                    });
                    logger.info("Unflattening the Json Message");
                    Map<String, Object> unflattenMap = JsonUnflattener.unflattenAsMap(mapper.writeValueAsString(flattenKeyMap));

                    unflattenMap.keySet().forEach(key -> {
                        Map<String, Object> value = (Map<String, Object>) unflattenMap.get(key);
                        value.keySet().forEach(innerKey -> {
                            medicationForm.put(innerKey, value.get(innerKey));
                        });
                    });

                    String bqSchema = getBqSchema(gcsClient, schemaBucketName, medicationForm);
                    String tableName = buildTableName(medicationForm);
                    addAuditFields(uuid, validRow);
                    medicationForm.forEach(validRow::set);
                    TableRowSpecs specs = new TableRowSpecs(tableName, bqSchema, bqDataset, message);
                    logger.info(String.format("Output Json is {%s}", validRow));
                    logger.info("Generating Valid Records");
                    processContext.output(validRecords, KV.of(validRow, specs));
                } catch (Exception ex) {
                    logger.error(String.format("Exception Occurred {%s}", ex));
                    TableRow invalidRow = extractDeadLetterEvent(message, uuid, ex);
                    String dead_letter_schema = BigQueryHelpers.toJsonString(deadLetterQueueBQSchema());
                    TableRowSpecs specs = new TableRowSpecs(retryDeadLetterQueue, dead_letter_schema, deadLetterQueue, message);
                    processContext.output(invalidRecords, KV.of(invalidRow, specs));
                }
            }

            private String getBqSchema(Storage gcsClient, String schemaBucketName, Map<String, Object> medicationForm) {
                String formType = (String) medicationForm.get("formName");
                logger.info("Getting Schemas from Schema Registry");
                formType = formType.toLowerCase().replace(" ", "_");
                String schemaVersion = (String) medicationForm.getOrDefault(VERSION, "1.0");
                String schemaPath = "schemas/" + formType + "_" + schemaVersion + ".json";
                Blob blob = gcsClient.get(BlobId.of(schemaBucketName, schemaPath));
                return new String(blob.getContent());
            }

            private String buildTableName(Map<String, Object> medicationForm) {
                logger.info("Building Table Name");
                String formType = (String) medicationForm.get("formName");
                formType = formType.toLowerCase().replace(" ", "_");
                String schemaVersion = (String) medicationForm.getOrDefault(VERSION, "1.0");
                schemaVersion = schemaVersion.replace(".", "_");
                return formType + "--" + schemaVersion;
            }

            private void addAuditFields(UUID uuid, TableRow validRow) {
                logger.info("Adding Audit Fields");
                validRow.set(GUID_COLUMN, uuid.toString());
                validRow.set(INSERTION_TIME_STAMP, Instant.now().toString());
            }


        }).withOutputTags(validRecords, TupleTagList.of(Arrays.asList(invalidRecords))));
    }
}
