package com.knoldus.pharmacy.options;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.Storage;
import com.google.crypto.tink.HybridDecrypt;
import com.google.crypto.tink.HybridEncrypt;
import com.knoldus.pharmacy.services.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface BigQueryOptions extends PubSubOptions {
    @Description("GCP project to access")
    @Validation.Required
    @Default.String("de-da-poc")
    String getGcpProject();

    void setGcpProject(String gcpProject);

    @Description("Location of the data catalog")
    @Validation.Required
    @Default.String("us-east1")
    String getLocation();

    void setLocation(String location);

    @Description("Tag Template ID")
    @Validation.Required
    @Default.String("pii")
    String getTagTemplate();

    void setTagTemplate(String tagTemplate);

    @Description("BigQuery Dataset")
    @Validation.Required
    @Default.String("medication_forms")
    String getBqDataset();

    void setBqDataset(String bqDataset);

    @Description("BigQuery Write Disposition")
    @Default.String("WRITE_APPEND")
    String getWriteDisposition();

    void setWriteDisposition(String writeDisposition);

    @Description("BigQuery Create Disposition")
    @Default.String("CREATE_IF_NEEDED")
    String getCreateDisposition();

    void setCreateDisposition(String createDisposition);

    @Description("BigQuery Dead Letter Queue dataset")
    @Default.String("dead_letter_queue")
    String getDeadLetterQueue();

    void setDeadLetterQueue(String deadLetterQueue);

    @Description("BigQuery Dead Letter Queue Table Name")
    @Default.String("dlq_event")
    String getDeadLetterQueueTableName();

    void setDeadLetterQueueTableName(String deadLetterQueueTableName);

    @Description("BigQuery Dead Letter Queue Table Name")
    @Default.String("dlq_event_retry")
    String getRetryDeadLetterQueue();

    void setRetryDeadLetterQueue(String retryDeadLetterQueue);
    @JsonIgnore
    @Description("Bigquery Client")
    @Default.InstanceFactory(BigQueryClientFactory.class)
    BigQuery getBigQueryClient();

    void setBigQueryClient(BigQuery bigQueryClient);

    @Description("Encrypted Private Key Secret")
    @Default.String("projects/605540203469/secrets/encrypted_private_key/versions/1")
    String getEncryptedPrivateKeySecret();

    void setEncryptedPrivateKeySecret(String encryptedPrivateKeySecret);

    @Description("GCS Bucket Name")
    @Default.String("pharamacy-deda-poc")
    String getGcsBucketName();

    void setGcsBucketName(String gcsBucketName);

    @Description("GCS File Name")
    @Default.String("encrypted-data/encryptedMessages")
    String getGcsFileName();

    void setGcsFileName(String gcsFileName);

    @Description("Key Encrypting Key Uri")
    @Default.String("gcp-kms://projects/de-da-poc/locations/global/keyRings/DE-DA-POC-Key-Ring/cryptoKeys/PharmacyDeDa-Key-Enc")
    String getKeyUri();

    void setKeyUri(String keyUri);

    @Description("Encrypted Public Key Secret")
    @Default.String("projects/605540203469/secrets/encrypted_public_key/versions/1")
    String getEncryptedPublicKeySecret();

    void setEncryptedPublicKeySecret(String encryptedPublicKeySecret);

    @JsonIgnore
    @Description("GCS Client")
    @Default.InstanceFactory(GcsClientFactory.class)
    Storage getGcsClient();

    void setGcsClient(Storage gcsClient);

    @Description("Schema Bucket")
    @Default.String("pharamacy-deda-poc")
    String getSchemaBucket();

    void setSchemaBucket(String schemaBucket);

    @Description("Default ObjectMapper instance")
    @Default.InstanceFactory(ObjectMapperInstanceFactory.class)
    ObjectMapper getDefaultObjectMapper();

    void setDefaultObjectMapper(ObjectMapper defaultObjectMapper);

    @Description("Default HybridEncrypt instance")
    @Default.InstanceFactory(HybridEncryptInstanceFactory.class)
    HybridEncrypt getDefaultHybridEncrypt();

    void setDefaultHybridEncrypt(HybridEncrypt defaultHybridEncrypt);

    @Description("Default HybridEncrypt instance")
    @Default.InstanceFactory(HybridDecryptInstanceFactory.class)
    HybridDecrypt getDefaultHybridDecrypt();

    void setDefaultHybridDecrypt(HybridDecrypt defaultHybridDecrypt);


}
