package com.knoldus.pharmacy.transformations;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.HybridDecrypt;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import com.knoldus.pharmacy.Application;
import com.knoldus.pharmacy.models.TableRowSpecs;
import com.knoldus.pharmacy.options.BigQueryOptions;
import com.knoldus.pharmacy.services.KmsService;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.*;

import static com.knoldus.pharmacy.Application.*;
import static com.knoldus.pharmacy.schema.BQJsonToBQSchema.deadLetterQueueBQSchema;
import static com.knoldus.pharmacy.utils.Utility.extractDeadLetterEvent;


public class ValidateRecords extends PTransform<PCollection<String>, PCollectionTuple> {
    final Logger logger = LoggerFactory.getLogger(ValidateRecords.class);


    @Override
    public PCollectionTuple expand(PCollection<String> input) {
        return input.apply("MapToRecord", ParDo.of(new DoFn<String, String>() {

                    @ProcessElement
                    public void process(ProcessContext processContext, PipelineOptions options) {
                        String message = null;
                        UUID uuid = UUID.randomUUID();
                        String base64EncryptedMessage = processContext.element();
                        BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);
                        String deadLetterQueue = bigQueryOptions.getDeadLetterQueue();
                        String deadLetterQueueTable = bigQueryOptions.getDeadLetterQueueTableName();
                        String retryDeadLetterQueue = bigQueryOptions.getRetryDeadLetterQueue();
                        try {

                            HybridDecrypt hybridDecrypt = bigQueryOptions.getDefaultHybridDecrypt();

                            String encryptedPrivateSecret = KmsService.getSecret(bigQueryOptions.getEncryptedPrivateKeySecret());

                            String privateSecret = KmsService.decryptKeys(encryptedPrivateSecret, bigQueryOptions.getKeyUri());

                            KeysetHandle privateHandle = CleartextKeysetHandle.read(JsonKeysetReader.withString(privateSecret));

                            message = new String(KmsService.hybridDecryption(hybridDecrypt, base64EncryptedMessage, privateHandle), StandardCharsets.UTF_8);
                            ObjectMapper mapper = bigQueryOptions.getDefaultObjectMapper();

                            TypeReference<HashMap<String, Object>> typeRef
                                    = new TypeReference<HashMap<String, Object>>() {
                            };
                            HashMap<String, Object> messageMap = mapper.readValue(message, typeRef);
                            if (!messageMap.containsKey("data")) {
                                throw new IOException("Missing Field Value data");
                            } else if (!messageMap.containsKey("info")) {
                                throw new IOException("Missing Field Value Info");
                            }
                            HashMap<String, Object> info = (HashMap<String, Object>) messageMap.get("info");
                            if (!info.containsKey("formName")) {
                                throw new IOException("Missing Field Value formName");
                            } else if (!info.containsKey("version")) {
                                throw new IOException("Missing Field Value version");
                            }
                            processContext.output(records, message);
                        } catch (
                                IOException ex) {
                            logger.error(String.format("IOException Occurred {%s}", ex));
                            TableRow invalidRow = extractDeadLetterEvent(message, uuid, ex);
                            String dead_letter_schema = BigQueryHelpers.toJsonString(deadLetterQueueBQSchema());
                            TableRowSpecs specs = new TableRowSpecs(deadLetterQueueTable, dead_letter_schema, deadLetterQueue, message);
                            processContext.output(invalidRecords, KV.of(invalidRow, specs));
                        } catch (
                                GeneralSecurityException ex) {
                            logger.error(String.format("GeneralSecurityException Occurred {%s}", ex));
                            TableRow invalidRow = extractDeadLetterEvent(base64EncryptedMessage, uuid, ex);
                            String dead_letter_schema = BigQueryHelpers.toJsonString(deadLetterQueueBQSchema());
                            TableRowSpecs specs = new TableRowSpecs(retryDeadLetterQueue, dead_letter_schema, deadLetterQueue, base64EncryptedMessage);
                            processContext.output(invalidRecords, KV.of(invalidRow, specs));
                        } catch (
                                Exception ex) {
                            logger.error(String.format("Exception Occurred {%s}", ex));
                            TableRow invalidRow = extractDeadLetterEvent(message, uuid, ex);
                            String dead_letter_schema = BigQueryHelpers.toJsonString(deadLetterQueueBQSchema());
                            TableRowSpecs specs = new TableRowSpecs(retryDeadLetterQueue, dead_letter_schema, deadLetterQueue, message);
                            processContext.output(invalidRecords, KV.of(invalidRow, specs));
                        }
                    }
                }).

                withOutputTags(records, TupleTagList.of(Arrays.asList(invalidRecords))));
    }
}
