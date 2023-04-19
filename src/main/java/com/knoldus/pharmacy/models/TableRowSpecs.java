package com.knoldus.pharmacy.models;

import java.io.Serializable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/** [[TableRowSpecs]] is a class, representing specifications of a BigQuery Table. */
@DefaultCoder(AvroCoder.class)
public class TableRowSpecs implements Serializable {

    private String dataset;
    private String bqTableName;
    private String bqSchema;

    public TableRowSpecs() {
    }

    public TableRowSpecs(
            String bqTableName, String bqSchema, String dataset) {
        this.bqTableName = bqTableName;
        this.bqSchema = bqSchema;
        this.dataset = dataset;
    }

    public String getBqTableName() {
        return bqTableName;
    }

    @Override
    public String toString() {
        return "TableRowSpecs{" +
                "dataset='" + dataset + '\'' +
                ", bqTableName='" + bqTableName + '\'' +
                ", bqSchema='" + bqSchema + '\'' +
                '}';
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public void setBqTableName(String bqTableName) {
        this.bqTableName = bqTableName;
    }

    public String getBqSchema() {
        return bqSchema;
    }

    public void setBqSchema(String bqSchema) {
        this.bqSchema = bqSchema;
    }


}

