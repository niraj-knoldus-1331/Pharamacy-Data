package com.knoldus.pharmacy.exceptions;

import java.io.IOException;

public class DataCatalogException extends IOException {
    /**
     * @param message [[String]] the custom exception message
     */
    public DataCatalogException(final String message) {
        super(message);
    }
}
