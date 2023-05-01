package com.knoldus.pharmacy.transformations;

import com.google.cloud.storage.*;
import com.knoldus.pharmacy.options.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SendtoGCS extends DoFn<String, Void> {

    @ProcessElement
    public void process(ProcessContext processContext, PipelineOptions options) {
        String element = processContext.element();
        BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);
        Storage storage = bigQueryOptions.getGcsClient();
        BlobId blobId = BlobId.of(bigQueryOptions.getGcsBucketName(), bigQueryOptions.getGcsFileName());
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        Blob blob = storage.create(blobInfo, element.getBytes(UTF_8));
    }
}
