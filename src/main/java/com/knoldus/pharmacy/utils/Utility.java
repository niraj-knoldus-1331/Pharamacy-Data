package com.knoldus.pharmacy.utils;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.knoldus.pharmacy.transformations.EncryptPIIColumns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.knoldus.pharmacy.utils.BeamPipelineConstants.MEDICATION_FORM_TYPE;

public class Utility {
    static final Logger logger = LoggerFactory.getLogger(EncryptPIIColumns.class);
    public static List<String> getPIIColumns(Storage gcsClient, String bucketName, String formName, String version) {
        logger.info("Getting PII Columns from Schema Registry");
        formName = formName.toLowerCase().replace(" ", "_");
        String piiColumnsPath = "pii/" + formName + "_" + version + ".txt";
        Optional<Blob> blob = Optional.ofNullable(gcsClient.get(BlobId.of(bucketName, piiColumnsPath)));
        return blob.map(value -> List.of(new String(value.getContent()).strip().split(","))).orElseGet(List::of);
    }
}