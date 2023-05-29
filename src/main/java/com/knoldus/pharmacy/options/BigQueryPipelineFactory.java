package com.knoldus.pharmacy.options;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.Preconditions;

public class BigQueryPipelineFactory {

    public static Pipeline createBigQueryPipeline(final String[] args) {

        PipelineOptionsFactory.register(BigQueryOptions.class);
        BigQueryOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .create()
                        .as(BigQueryOptions.class);

        Preconditions.checkArgumentNotNull(options.getInputTopic());

        return Pipeline.create(options);
    }
}
