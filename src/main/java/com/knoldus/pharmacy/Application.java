package com.knoldus.pharmacy;


import com.google.api.services.bigquery.model.TableRow;
import com.knoldus.pharmacy.models.TableRowSpecs;
import com.knoldus.pharmacy.options.BigQueryOptions;
import com.knoldus.pharmacy.options.BigQueryPipelineFactory;
import com.knoldus.pharmacy.services.BigQueryService;
import com.knoldus.pharmacy.transformations.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.knoldus.pharmacy.utils.BeamPipelineConstants.INVALID_RECORDS_INSERTION_TF;
import static com.knoldus.pharmacy.utils.BeamPipelineConstants.VALID_RECORDS_INSERTION_TF;


public class Application {
    public static final TupleTag<KV<TableRow, TableRowSpecs>> validRecords = new TupleTag<>();
    public static final TupleTag<KV<TableRow, TableRowSpecs>> invalidRecords = new TupleTag<>();
    final static Logger logger = LoggerFactory.getLogger(Application.class);


    public static void main(String[] args) {

        logger.info("Creating BigQuery Pipeline");
        Pipeline pipeline = BigQueryPipelineFactory.createBigQueryPipeline(args);
        BigQueryOptions options = pipeline.getOptions().as(BigQueryOptions.class);
        BigQueryIO.Write.CreateDisposition createDisposition =
                BigQueryIO.Write.CreateDisposition.valueOf(options.getCreateDisposition());
        BigQueryIO.Write.WriteDisposition writeDisposition =
                BigQueryIO.Write.WriteDisposition.valueOf(options.getWriteDisposition());

        PCollection<@UnknownKeyFor @NonNull @Initialized PubsubMessage> messages;

        if (options.getUseSubscription()) {
            logger.info("Reading From Subscription");
            messages = pipeline.apply("Reading From Subscription",
                    PubsubIO.readMessagesWithAttributes()
                            .fromSubscription(options.getInputSubscription()));
        } else {
            logger.info("Reading From Pub Sub Topic");
            messages = pipeline
                    // 1) Read string messages from a Pub/Sub topic.
                    .apply("Read PubSub Messages From Topic", PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
        }

        PCollection<String> encryptedMessages = messages.apply("Encrypt the Messages", new HybridEncryption());
        encryptedMessages.apply("Write to GCS bucket", ParDo.of(new SendtoGCS()));

        PCollectionTuple convertMessagesToTableRowTuple = encryptedMessages.apply("Convert Encrypted Messages to Table Row", new PubSubMessagetoTableRow());
        PCollection<KV<TableRow, TableRowSpecs>> validPcollection = convertMessagesToTableRowTuple.get(validRecords).
                setCoder(KvCoder.of(TableRowJsonCoder.of(), AvroCoder.of(TableRowSpecs.class)));
        PCollection<KV<TableRow, TableRowSpecs>> attachingPiiTag = validPcollection.apply("Attaching PII Tag", ParDo.of(new PIITagAttacher()));
        PCollection<KV<TableRow, TableRowSpecs>> encryptingPiiColumns = attachingPiiTag.apply("Encrypting PII columns", ParDo.of(new EncryptPIIColumns()));
        BigQueryService bigQueryService = new BigQueryService(options.getGcpProject());

        bigQueryService.insertToBigQuery(encryptingPiiColumns, createDisposition, writeDisposition, VALID_RECORDS_INSERTION_TF);
        PCollection<KV<TableRow, TableRowSpecs>> invalidPCollection = convertMessagesToTableRowTuple.get(invalidRecords).setCoder(KvCoder.of(TableRowJsonCoder.of(), AvroCoder.of(TableRowSpecs.class)));
        bigQueryService.insertToBigQueryDeadLetterQueue(invalidPCollection, INVALID_RECORDS_INSERTION_TF);
        pipeline.run();
    }
}
