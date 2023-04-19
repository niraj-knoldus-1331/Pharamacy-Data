package com.knoldus.pharmacy.services;

import com.google.crypto.tink.HybridEncrypt;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class HybridEncryptInstanceFactory implements DefaultValueFactory<HybridEncrypt> {
    @Override
    public HybridEncrypt create(@UnknownKeyFor @NonNull @Initialized PipelineOptions options) {
        return (plaintext, contextInfo) -> new byte[0];
    }
}
