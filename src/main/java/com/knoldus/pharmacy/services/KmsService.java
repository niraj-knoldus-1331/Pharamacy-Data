package com.knoldus.pharmacy.services;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.crypto.tink.*;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.aead.KmsEnvelopeAeadKeyManager;
import com.google.crypto.tink.hybrid.HybridConfig;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Optional;

public class KmsService {
    static final Logger logger = LoggerFactory.getLogger(KmsService.class);

    public static String getSecret(String secretID) throws IOException {
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            AccessSecretVersionResponse response = client.accessSecretVersion(secretID);
            return response.getPayload().getData().toStringUtf8();
        }
    }

    public static String decryptKeys(String keys, String keyUri) throws GeneralSecurityException {
        AeadConfig.register();
        try {
            GcpKmsClient.register(Optional.of(keyUri), Optional.empty());
        } catch (GeneralSecurityException ex) {
            logger.error("Error initializing GCP client");

        }
        Aead aead = null;
        try {
            KeysetHandle handle = KeysetHandle.generateNew(KmsEnvelopeAeadKeyManager.createKeyTemplate(keyUri, KeyTemplates.get("AES256_GCM")));
            aead = handle.getPrimitive(Aead.class);
        } catch (GeneralSecurityException ex) {
            logger.error("Error initializing GCP client");
        }
        byte[] decoded = Base64.getDecoder().decode(keys.getBytes(StandardCharsets.UTF_8));
        byte[] decrypted = aead.decrypt(decoded, null);

        return new String(decrypted, StandardCharsets.ISO_8859_1);
    }

    public static byte[] hybridEncryption(HybridEncrypt encryptor, KeysetHandle publicHandle, String value) throws GeneralSecurityException {
        HybridConfig.register();
        try {
            encryptor = publicHandle.getPrimitive(HybridEncrypt.class);
        } catch (GeneralSecurityException ex) {
            logger.error("Cannot create primitive");
        }
        byte[] encrypt = encryptor.encrypt(value.getBytes(), null);
        return Base64.getEncoder().encode(encrypt);
    }

    public static byte[] hybridDecryption(HybridDecrypt hybridDecrypt, String base64EncryptedMessage, KeysetHandle privateHandle) throws GeneralSecurityException {
        byte[] hybridEncryptedMessage = Base64.getDecoder().decode(base64EncryptedMessage);
        hybridDecrypt = privateHandle.getPrimitive(HybridDecrypt.class);
        return hybridDecrypt.decrypt(hybridEncryptedMessage, null);
    }
}
