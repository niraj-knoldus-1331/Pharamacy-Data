package com.knoldus.pharmacy;

import com.google.cloud.secretmanager.v1.*;
import com.google.crypto.tink.*;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.aead.KmsEnvelopeAeadKeyManager;
import com.google.crypto.tink.hybrid.HybridConfig;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Optional;

public class GenerateAsymmetricKeys {
    final static Logger logger = LoggerFactory.getLogger(GenerateAsymmetricKeys.class);
    final static String PROJECT_ID = "de-da-ml";
    final static String ENCRYPTED_PRIVATE_SECRET_KEY = "encrypted_private_key";
    final static String ENCRYPTED_PUBLIC_SECRET_KEY = "encrypted_public_key";
    final static String KEK_URI = "gcp-kms://projects/de-da-ml/locations/global/keyRings/PharmacyDeDa-KeyRing/cryptoKeys/PharmacyDeDa-Key-Enc";

    public static void main(String[] args) throws Exception {
        HybridConfig.register();
        KeysetHandle privateKeysetHandle = KeysetHandle.generateNew(
                KeyTemplates.get("ECIES_P256_COMPRESSED_HKDF_HMAC_SHA256_AES128_GCM"));

        KeysetHandle publicKeysetHandle =
                privateKeysetHandle.getPublicKeysetHandle();

        ByteArrayOutputStream privateKey = new ByteArrayOutputStream();
        KeysetWriter privateKeySetWriter = JsonKeysetWriter.withOutputStream(privateKey);
        CleartextKeysetHandle.write(privateKeysetHandle, privateKeySetWriter);

        ByteArrayOutputStream publicKey = new ByteArrayOutputStream();
        KeysetWriter publicKeysetWriter = JsonKeysetWriter.withOutputStream(publicKey);
        CleartextKeysetHandle.write(publicKeysetHandle, publicKeysetWriter);

        String privateKeyEncrypted = encryptKeys(String.valueOf(privateKey));
        String publicKeyEncrypted = encryptKeys(String.valueOf(publicKey));

        createSecret(PROJECT_ID, ENCRYPTED_PRIVATE_SECRET_KEY, privateKeyEncrypted);
        createSecret(PROJECT_ID, ENCRYPTED_PUBLIC_SECRET_KEY, publicKeyEncrypted);
    }

    public static void createSecret(String projectId, String secretId, String data) throws Exception {
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            ProjectName projectName = ProjectName.of(projectId);
            Secret secret =
                    Secret.newBuilder()
                            .setReplication(
                                    Replication.newBuilder()
                                            .setAutomatic(Replication.Automatic.newBuilder().build())
                                            .build())
                            .build();

            Secret createdSecret = client.createSecret(projectName, secretId, secret);
            SecretPayload payload =
                    SecretPayload.newBuilder().setData(ByteString.copyFromUtf8(data)).build();
            SecretVersion addedVersion = client.addSecretVersion(createdSecret.getName(), payload);
        }
    }

    public static String encryptKeys(String keys) throws GeneralSecurityException {
        AeadConfig.register();
        try {
            GcpKmsClient.register(Optional.of(KEK_URI), Optional.empty());
        } catch (GeneralSecurityException ex) {
            logger.error("Error initializing GCP client");
        }
        Aead aead = null;
        try {
            KeysetHandle handle =
                    KeysetHandle.generateNew(
                            KmsEnvelopeAeadKeyManager.createKeyTemplate(KEK_URI, KeyTemplates.get("AES256_GCM")));
            aead = handle.getPrimitive(Aead.class);
        } catch (GeneralSecurityException ex) {
            logger.error("Error initializing GCP client");
        }

        byte[] encrypt = aead.encrypt(keys.getBytes(StandardCharsets.ISO_8859_1), null);
        byte[] encode = Base64.getEncoder().encode(encrypt);
        return new String(encode,StandardCharsets.UTF_8);
    }
}

