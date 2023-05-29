package com.knoldus.pharmacy.transformations;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.knoldus.pharmacy.models.TableRowSpecs;
import com.knoldus.pharmacy.options.BigQueryOptions;
import com.knoldus.pharmacy.services.BigQueryService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import static com.knoldus.pharmacy.schema.BQJsonToBQSchema.deadLetterQueueBQSchema;
import static com.knoldus.pharmacy.utils.BeamPipelineConstants.*;
import static com.knoldus.pharmacy.utils.BeamPipelineConstants.INSERTION_TIME_STAMP;

public class FailedRecordsTransformation extends PTransform<PCollection<BigQueryInsertError>, PCollection<KV<TableRow, TableRowSpecs>>> {

    final static Logger logger = LoggerFactory.getLogger(FailedRecordsTransformation.class);

    @Override
    public PCollection<KV<TableRow, TableRowSpecs>> expand(PCollection<BigQueryInsertError> input) {
        return input.apply(ParDo.of(new DoFn<BigQueryInsertError, KV<TableRow, TableRowSpecs>>() {

            @ProcessElement
            public void process(ProcessContext processContext, PipelineOptions options) throws IOException {
                BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);
                logger.info("Processing Failed Inserts");
                UUID uuid = UUID.randomUUID();
                String dead_letter_schema = BigQueryHelpers.toJsonString(deadLetterQueueBQSchema());
                String deadLetterQueue = bigQueryOptions.getDeadLetterQueue();
                String deadLetterQueueTable = bigQueryOptions.getDeadLetterQueueTableName();

                TableRow tableRow = Objects.requireNonNull(processContext.element()).getRow();
                TableReference table = Objects.requireNonNull(processContext.element()).getTable();

                String tableName = String.format(
                        "%s:%s.%s",
                        table.getProjectId(), table.getDatasetId(), table.getTableId());
                StringBuilder errors = new StringBuilder();
                Objects.requireNonNull(processContext.element())
                        .getError()
                        .getErrors()
                        .forEach(errorProto -> errors.append(errorProto.getMessage()));

                String errorMessage = String.format("%s %s %s", errors, "for the table", tableName);
                logger.error(String.format("Error During Insertion {%s}", errorMessage));
                TableRow invalidRow = new TableRow();
                invalidRow.set(MESSAGE_COLUMN, tableRow.toPrettyString());
                invalidRow.set(EXCEPTION_MESSAGE_COLUMN, errorMessage);
                invalidRow.set(GUID_COLUMN, uuid.toString());
                invalidRow.set(INSERTION_TIME_STAMP, Instant.now().toString());
                TableRowSpecs specs = new TableRowSpecs(deadLetterQueueTable, dead_letter_schema, deadLetterQueue, tableRow.toPrettyString());
                processContext.output(KV.of(invalidRow, specs));
            }
        }));
    }
}
