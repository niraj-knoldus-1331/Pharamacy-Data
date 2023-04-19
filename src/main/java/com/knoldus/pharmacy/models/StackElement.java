package com.knoldus.pharmacy.models;

import java.util.Map;
import java.util.Optional;

public class StackElement {
    public Optional<String> key;
    public Map<String, ?> elements;

    public StackElement(String key, Map<String, ?> elements) {
        this.key = Optional.ofNullable(key);
        this.elements = elements;
    }
}
