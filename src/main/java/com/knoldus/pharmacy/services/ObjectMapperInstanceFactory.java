package com.knoldus.pharmacy.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class ObjectMapperInstanceFactory implements DefaultValueFactory<ObjectMapper> {
    @Override
    public ObjectMapper create(@UnknownKeyFor @NonNull @Initialized PipelineOptions options) {
        return new ObjectMapper();
    }
}
