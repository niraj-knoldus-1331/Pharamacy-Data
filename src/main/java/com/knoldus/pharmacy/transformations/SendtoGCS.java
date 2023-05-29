package com.knoldus.pharmacy.transformations;

import com.google.cloud.storage.*;
import com.knoldus.pharmacy.options.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SendtoGCS extends DoFn<String, Void> {
    final Logger logger = LoggerFactory.getLogger(SendtoGCS.class);

    @ProcessElement
    public void process(ProcessContext processContext, PipelineOptions options) {
        try {
            String element = processContext.element();
            BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);
            Storage storage = bigQueryOptions.getGcsClient();
            BlobId blobId = BlobId.of(bigQueryOptions.getGcsBucketName(), bigQueryOptions.getGcsFileName());
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
            Blob blob = storage.create(blobInfo, element.getBytes(UTF_8));
        } catch (Exception ex) {
            logger.error(String.format("Exception Occurred {%s}", ex));
        }
    }
}
