package com.knoldus.pharmacy.transformations;

import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.HybridEncrypt;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import com.knoldus.pharmacy.options.BigQueryOptions;
import com.knoldus.pharmacy.services.KmsService;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;

public class HybridEncryption extends PTransform<PCollection<PubsubMessage>, PCollection<String>> {

    final Logger logger = LoggerFactory.getLogger(HybridEncryption.class);

    @Override
    public PCollection<String> expand(PCollection<PubsubMessage> input) {
        return input.apply("Encrypt", ParDo.of(new DoFn<PubsubMessage, String>() {
            @ProcessElement
            public void process(ProcessContext processContext, PipelineOptions options) throws GeneralSecurityException, IOException {
                BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);
                HybridEncrypt hybridEncrypt = bigQueryOptions.getDefaultHybridEncrypt();
                String encryptedPublicSecret = KmsService.getSecret(bigQueryOptions.getEncryptedPublicKeySecret());
                String publicSecret = KmsService.decryptKeys(encryptedPublicSecret, bigQueryOptions.getKeyUri());
                KeysetHandle publicHandle = CleartextKeysetHandle.read(JsonKeysetReader.withString(publicSecret));

                PubsubMessage pubsubMessage = processContext.element();
                String messages = new String(pubsubMessage.getPayload(), StandardCharsets.ISO_8859_1);
                String base64EncodedMessage = new String(KmsService.hybridEncryption(hybridEncrypt, publicHandle, messages), StandardCharsets.UTF_8);
                processContext.output(base64EncodedMessage);
            }
        }));
    }
}
