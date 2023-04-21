package com.knoldus.pharmacy.utils;

public class BeamPipelineConstants {

    public static final String MESSAGE_COLUMN = "PubSubMessage";

    public static final String VERSION = "version";
    public static final String EXCEPTION_MESSAGE_COLUMN = "ExceptionMessage";
    public static String MEDICATION_FORM_TYPE = "Medication Review";
    public static final String GUID_COLUMN = "guid";
    public static final String INSERTION_TIME_STAMP = "insertion_time_stamp";

    public static final String COLUMN_TYPE = "STRING";
    public static final String COLUMN_TYPE_TIMESTAMP = "TIMESTAMP";
    public static final String COLUMN_MODE_REQUIRED = "REQUIRED";
    public static final String COLUMN_MODE_NULLABLE = "NULLABLE";

    public static final String VALID_RECORDS_INSERTION_TF = "Valid Records Insertion";
    public static final String CONTAIN_PII = "contains_pii";

    public static final String INVALID_RECORDS_INSERTION_TF = "Invalid Records Insertion";

    public static final String FAILED_RECORDS_INSERTION_TF = "Failed Records Insertion";

    public static final String LINKED_RESOURCE_FORMAT = "//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s";

}
