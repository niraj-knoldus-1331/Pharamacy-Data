package com.knoldus.pharmacy.services;

import com.google.crypto.tink.HybridDecrypt;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.security.GeneralSecurityException;

public class HybridDecryptInstanceFactory implements DefaultValueFactory<HybridDecrypt> {
    @Override
    public HybridDecrypt create(@UnknownKeyFor @NonNull @Initialized PipelineOptions options) {
        return (ciphertext, contextInfo) -> new byte[0];
    }
}
