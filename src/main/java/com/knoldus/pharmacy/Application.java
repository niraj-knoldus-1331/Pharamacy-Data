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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.knoldus.pharmacy.utils.BeamPipelineConstants.INVALID_RECORDS_INSERTION_TF;
import static com.knoldus.pharmacy.utils.BeamPipelineConstants.VALID_RECORDS_INSERTION_TF;

public class Application {
    public static final TupleTag<KV<TableRow, TableRowSpecs>> validRecords = new TupleTag<>();
    public static final TupleTag<String> records = new TupleTag<>();
    public static final TupleTag<KV<TableRow, TableRowSpecs>> invalidRecords = new TupleTag<>();
    final static Logger logger = LoggerFactory.getLogger(Application.class);


    public static void main(String[] args) {

        logger.info("Creating BigQuery Pipeline");
        Pipeline pipeline = BigQueryPipelineFactory.createBigQueryPipeline(args);
        BigQueryOptions options = pipeline.getOptions().as(BigQueryOptions.class);
        BigQueryIO.Write.CreateDisposition createDisposition = BigQueryIO.Write.CreateDisposition.valueOf(options.getCreateDisposition());
        BigQueryIO.Write.WriteDisposition writeDisposition = BigQueryIO.Write.WriteDisposition.valueOf(options.getWriteDisposition());

        PCollection<@UnknownKeyFor @NonNull @Initialized PubsubMessage> messages;

        if (options.getUseSubscription()) {
            logger.info("Reading From Subscription");
            messages = pipeline.apply("Reading From Subscription", PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()));
        } else {
            logger.info("Reading From Pub Sub Topic");
            messages = pipeline
                    // 1) Read string messages from a Pub/Sub topic.
                    .apply("Read PubSub Messages From Topic", PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
        }

        PCollection<String> encryptedMessages = messages.apply("Encrypt the Messages", new HybridEncryption());
        encryptedMessages.apply("Write to GCS bucket", ParDo.of(new SendtoGCS()));

        PCollectionTuple validatingRecords = encryptedMessages.apply("Validating Records", new ValidateRecords());

        PCollection<String> validPCollection = validatingRecords.get(records).setCoder(StringUtf8Coder.of());
        PCollectionTuple convertMessagesToTableRowTuple = validPCollection
                .apply("Convert Valid Messages to Table Row", new PubSubMessagetoTableRow());

        PCollection<KV<TableRow, TableRowSpecs>> validKvpCollection = convertMessagesToTableRowTuple.get(validRecords).
                setCoder(KvCoder.of(TableRowJsonCoder.of(), AvroCoder.of(TableRowSpecs.class)));
        PCollectionTuple attachingPiiTagTuple = validKvpCollection.
                apply("Attaching PII Tag", ParDo.of(new PIITagAttacher()).
                        withOutputTags(validRecords, TupleTagList.of(Arrays.asList(invalidRecords))));

        PCollection<KV<TableRow, TableRowSpecs>> validAttachingPIITagCollection = attachingPiiTagTuple.get(validRecords).
                setCoder(KvCoder.of(TableRowJsonCoder.of(), AvroCoder.of(TableRowSpecs.class)));
        PCollectionTuple encryptingPiiColumnsTuple = validAttachingPIITagCollection
                .apply("Encrypting PII columns", ParDo.of(new EncryptPIIColumns())
                        .withOutputTags(validRecords, TupleTagList.of(Arrays.asList(invalidRecords))));

        PCollection<KV<TableRow, TableRowSpecs>> validEncryptPIIColumns = encryptingPiiColumnsTuple.get(validRecords).
                setCoder(KvCoder.of(TableRowJsonCoder.of(), AvroCoder.of(TableRowSpecs.class)));
        BigQueryService bigQueryService = new BigQueryService(options.getGcpProject());
        bigQueryService.insertToBigQuery(validEncryptPIIColumns, createDisposition, writeDisposition, VALID_RECORDS_INSERTION_TF);

        PCollection<KV<TableRow, TableRowSpecs>> invalidPCollection = validatingRecords.get(invalidRecords).setCoder(KvCoder.of(TableRowJsonCoder.of(), AvroCoder.of(TableRowSpecs.class)));
        PCollection<KV<TableRow, TableRowSpecs>> invalidkvpCollection = convertMessagesToTableRowTuple.get(invalidRecords).setCoder(KvCoder.of(TableRowJsonCoder.of(), AvroCoder.of(TableRowSpecs.class)));
        PCollection<KV<TableRow, TableRowSpecs>> invalidAttachingPIITagCollection = attachingPiiTagTuple.get(invalidRecords).setCoder(KvCoder.of(TableRowJsonCoder.of(), AvroCoder.of(TableRowSpecs.class)));
        PCollection<KV<TableRow, TableRowSpecs>> invalidEncryptPIIColumns = encryptingPiiColumnsTuple.get(invalidRecords).setCoder(KvCoder.of(TableRowJsonCoder.of(), AvroCoder.of(TableRowSpecs.class)));


        PCollection<KV<TableRow, TableRowSpecs>> pCollection = PCollectionList.of(invalidPCollection)
                .and(invalidkvpCollection).and(invalidAttachingPIITagCollection).and(invalidEncryptPIIColumns).apply(Flatten.pCollections());
        bigQueryService.insertToBigQueryDeadLetterQueue(pCollection, INVALID_RECORDS_INSERTION_TF);
        pipeline.run();
    }
}
