package com.knoldus.pharmacy.transformations;


import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.knoldus.pharmacy.models.TableRowSpecs;
import com.knoldus.pharmacy.services.BigQueryService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class DynamicTableDestinations extends DynamicDestinations<KV<TableRow, TableRowSpecs>, TableRowSpecs> {
    final static Logger logger = LoggerFactory.getLogger(DynamicTableDestinations.class);
    private final String gcpProject;

    public DynamicTableDestinations(String gcpProject) {
        this.gcpProject = gcpProject;
    }

    @Override
    public TableRowSpecs getDestination(@Nullable @UnknownKeyFor @Initialized ValueInSingleWindow<KV<TableRow, TableRowSpecs>> element) {
        return Objects.requireNonNull(element.getValue()).getValue();
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized TableDestination getTable(TableRowSpecs destination) {
        String table = String.format("%s:%s.%s", this.gcpProject, destination.getDataset(), destination.getBqTableName());
        logger.info(String.format("Inserting Records into {%s}", table));
        return new TableDestination(table, "Output Table");
    }

    @Override
    public @Nullable @UnknownKeyFor @Initialized TableSchema getSchema(TableRowSpecs destination) {

        return BigQueryHelpers.fromJsonString(destination.getBqSchema(), TableSchema.class);
    }
}
