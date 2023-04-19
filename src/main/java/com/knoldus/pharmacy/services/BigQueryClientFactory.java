package com.knoldus.pharmacy.services;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class BigQueryClientFactory implements DefaultValueFactory<BigQuery> {
    @Override
    public BigQuery create(@UnknownKeyFor @NonNull @Initialized final PipelineOptions options) {
        return BigQueryOptions.getDefaultInstance().getService();
    }
}
