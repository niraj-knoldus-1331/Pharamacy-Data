package com.knoldus.pharmacy.transformations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.datacatalog.v1.DataCatalogClient;
import com.google.cloud.datacatalog.v1.Entry;
import com.google.cloud.datacatalog.v1.LookupEntryRequest;
import com.google.cloud.datacatalog.v1.Tag;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.HybridEncrypt;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.knoldus.pharmacy.exceptions.DataCatalogException;
import com.knoldus.pharmacy.models.TableRowSpecs;
import com.knoldus.pharmacy.options.BigQueryOptions;
import com.knoldus.pharmacy.services.KmsService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.*;

import static com.knoldus.pharmacy.Application.invalidRecords;
import static com.knoldus.pharmacy.schema.BQJsonToBQSchema.deadLetterQueueBQSchema;
import static com.knoldus.pharmacy.utils.BeamPipelineConstants.LINKED_RESOURCE_FORMAT;
import static com.knoldus.pharmacy.utils.Utility.extractDeadLetterEvent;
import static com.knoldus.pharmacy.utils.Utility.getPIIColumns;

public class EncryptPIIColumns extends DoFn<KV<TableRow, TableRowSpecs>, KV<TableRow, TableRowSpecs>> {
    final Logger logger = LoggerFactory.getLogger(EncryptPIIColumns.class);

    @ProcessElement
    public void process(ProcessContext context, PipelineOptions options) {
        BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);
        KV<TableRow, TableRowSpecs> element = context.element();
        TableRowSpecs tableRowSpecs = element.getValue();
        TableRow tableRow = element.getKey();
        String message = tableRowSpecs.getMessage();
        String retryDeadLetterQueue = bigQueryOptions.getRetryDeadLetterQueue();
        String deadLetterQueue = bigQueryOptions.getDeadLetterQueue();
        UUID uuid = UUID.randomUUID();
        try {
            HybridEncrypt hybridEncrypt = bigQueryOptions.getDefaultHybridEncrypt();
            String encryptedPublicSecret = KmsService.getSecret(bigQueryOptions.getEncryptedPublicKeySecret());
            String publicSecret = KmsService.decryptKeys(encryptedPublicSecret, bigQueryOptions.getKeyUri());
            KeysetHandle publicHandle = CleartextKeysetHandle.read(JsonKeysetReader.withString(publicSecret));
            List<String> piiColumns;
            TableRow updatedTableRow = new TableRow();
            Gson gson = new GsonBuilder().serializeNulls().create();
            String json = gson.toJson(tableRow);
            ObjectMapper mapper = bigQueryOptions.getDefaultObjectMapper();
            Map<String, Object> flattenJson = JsonFlattener.flattenAsMap(json);
            Map<String, Object> treeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            treeMap.putAll(flattenJson);
            boolean tableExists = tableExists(tableRowSpecs.getDataset(), tableRowSpecs.getBqTableName(), bigQueryOptions.getBigQueryClient());
            if (tableExists) {
                piiColumns = lookupEntry(bigQueryOptions.getGcpProject(), tableRowSpecs.getDataset(), tableRowSpecs.getBqTableName());
            } else {
                String formName = (String) tableRow.get("formName");
                String version = (String) tableRow.get("version");
                piiColumns = getPIIColumns(bigQueryOptions.getGcsClient(), bigQueryOptions.getSchemaBucket(), formName, version);
            }

            for (String column : piiColumns) {
                String columnValue = (String) treeMap.get(column);
                if (columnValue != null) {
                    String base64EncodedColumn = new String(KmsService.hybridEncryption(hybridEncrypt, publicHandle, columnValue), StandardCharsets.UTF_8);
                    treeMap.put(column, base64EncodedColumn);
                }
            }
            Map<String, Object> unflattenMap = JsonUnflattener.unflattenAsMap(mapper.writeValueAsString(treeMap));
            unflattenMap.forEach(updatedTableRow::set);
            context.output(KV.of(updatedTableRow, tableRowSpecs));
        } catch (
                IOException ex) {
            logger.error(String.format("IOException Occurred {%s}", ex));
            TableRow invalidRow = extractDeadLetterEvent(message, uuid, ex);
            String dead_letter_schema = BigQueryHelpers.toJsonString(deadLetterQueueBQSchema());
            TableRowSpecs specs = new TableRowSpecs(retryDeadLetterQueue, dead_letter_schema, deadLetterQueue, message);
            context.output(invalidRecords, KV.of(invalidRow, specs));
        } catch (
                GeneralSecurityException ex) {
            logger.error(String.format("GeneralSecurityException Occurred {%s}", ex));
            TableRow invalidRow = extractDeadLetterEvent(message, uuid, ex);
            String dead_letter_schema = BigQueryHelpers.toJsonString(deadLetterQueueBQSchema());
            TableRowSpecs specs = new TableRowSpecs(retryDeadLetterQueue, dead_letter_schema, deadLetterQueue, message);
            context.output(invalidRecords, KV.of(invalidRow, specs));
        } catch (
                Exception ex) {
            logger.error(String.format("Exception Occurred {%s}", ex));
            TableRow invalidRow = extractDeadLetterEvent(message, uuid, ex);
            String dead_letter_schema = BigQueryHelpers.toJsonString(deadLetterQueueBQSchema());
            TableRowSpecs specs = new TableRowSpecs(retryDeadLetterQueue, dead_letter_schema, deadLetterQueue, message);
            context.output(invalidRecords, KV.of(invalidRow, specs));
        }
    }


    public List<String> lookupEntry(String projectId, String datasetId, String tableName) throws IOException {
        List<String> piiColumns = new ArrayList<>();
        String linkedResource =
                String.format(LINKED_RESOURCE_FORMAT, projectId, datasetId, tableName);
        LookupEntryRequest request =
                LookupEntryRequest.newBuilder().setLinkedResource(linkedResource).build();
        try (DataCatalogClient dataCatalogClient = DataCatalogClient.create()) {
            Entry entry = dataCatalogClient.lookupEntry(request);
            DataCatalogClient.ListTagsPagedResponse listTagsPagedResponse = dataCatalogClient.listTags(entry.getName());
            for (Tag tag : listTagsPagedResponse.iterateAll()) {
                piiColumns.add(tag.getColumn());
            }

            return piiColumns;
        } catch (Exception ex) {
            throw new DataCatalogException(ex.getMessage());
        }

    }

    public boolean tableExists(String datasetName, String tableName, BigQuery bigQuery) {
        try {
            Table table = bigQuery.getTable(TableId.of(datasetName, tableName));
            return table != null
                    && table
                    .exists();
        } catch (BigQueryException e) {
            return false;
        }
    }
}
