package com.knoldus.pharmacy.services;

import com.google.api.services.bigquery.model.TableRow;
import com.knoldus.pharmacy.models.TableRowSpecs;
import com.knoldus.pharmacy.transformations.DynamicTableDestinations;
import com.knoldus.pharmacy.transformations.FailedRecordsTransformation;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.knoldus.pharmacy.utils.BeamPipelineConstants.FAILED_RECORDS_INSERTION_TF;

public class BigQueryService {

    private final String gcpProject;
    final static Logger logger = LoggerFactory.getLogger(BigQueryService.class);

    public BigQueryService(
            final String gcpProject) {
        this.gcpProject = gcpProject;
    }

    public void insertToBigQuery(
            PCollection<KV<TableRow, TableRowSpecs>> inputCollection,
            final BigQueryIO.Write.CreateDisposition createDisposition,
            final BigQueryIO.Write.WriteDisposition writeDisposition, String transformationName) {

        WriteResult writeResult =
                inputCollection
                        .apply(transformationName,
                                BigQueryIO.<KV<TableRow, TableRowSpecs>>write()
                                        .to(new DynamicTableDestinations(this.gcpProject))
                                        .withCreateDisposition(createDisposition)
                                        .withWriteDisposition(writeDisposition)
                                        .withFormatFunction((KV::getKey))
                                        .withExtendedErrorInfo()
                                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));


        PCollection<KV<TableRow, TableRowSpecs>> failedInserts = getFailedInserts(writeResult);
        insertToBigQueryDeadLetterQueue(failedInserts, FAILED_RECORDS_INSERTION_TF);
    }

    public void insertToBigQueryDeadLetterQueue(
            final PCollection<KV<TableRow, TableRowSpecs>> failedInsertRecords, String transformationName
    ) {

        failedInsertRecords
                .apply(transformationName,
                        BigQueryIO.<KV<TableRow, TableRowSpecs>>write()
                                .to(new DynamicTableDestinations(this.gcpProject))
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withFormatFunction((KV::getKey)));
    }


    private PCollection<KV<TableRow, TableRowSpecs>> getFailedInserts(
            final WriteResult writeResult) {
        return writeResult
                .getFailedInsertsWithErr()
                .apply(new FailedRecordsTransformation());
    }
}
