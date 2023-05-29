package com.knoldus.pharmacy.utils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.knoldus.pharmacy.transformations.EncryptPIIColumns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

import static com.knoldus.pharmacy.utils.BeamPipelineConstants.*;

public class Utility {
    static final Logger logger = LoggerFactory.getLogger(Utility.class);
    public static List<String> getPIIColumns(Storage gcsClient, String bucketName, String formName, String version) {
        logger.info("Getting PII Columns from Schema Registry");
        formName = formName.toLowerCase().replace(" ", "_");
        String piiColumnsPath = "pii/" + formName + "_" + version + ".txt";
        Optional<Blob> blob = Optional.ofNullable(gcsClient.get(BlobId.of(bucketName, piiColumnsPath)));
        return blob.map(value -> List.of(new String(value.getContent()).strip().split(","))).orElseGet(List::of);
    }

    public static TableRow extractDeadLetterEvent(String message, UUID uuid, Exception ex) {
        logger.info("Extracting Dead Letter Event");
        TableRow invalidRow = new TableRow();
        invalidRow.set(MESSAGE_COLUMN, message);
        invalidRow.set(EXCEPTION_MESSAGE_COLUMN, ex.getMessage());
        invalidRow.set(GUID_COLUMN, uuid.toString());
        invalidRow.set(INSERTION_TIME_STAMP, Instant.now().toString());
        return invalidRow;
    }

}