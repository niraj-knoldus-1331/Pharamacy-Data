package com.knoldus.pharmacy.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.knoldus.pharmacy.utils.BeamPipelineConstants;

import java.util.ArrayList;
import java.util.List;

public class BQJsonToBQSchema {

    public static TableSchema deadLetterQueueBQSchema() {
        List<TableFieldSchema> fieldsList = new ArrayList<>();

        fieldsList.add(
                new TableFieldSchema()
                        .setName(BeamPipelineConstants.MESSAGE_COLUMN)
                        .setType(BeamPipelineConstants.COLUMN_TYPE)
                        .setMode(BeamPipelineConstants.COLUMN_MODE_NULLABLE));
        fieldsList.add(
                new TableFieldSchema()
                        .setName(BeamPipelineConstants.EXCEPTION_MESSAGE_COLUMN)
                        .setType(BeamPipelineConstants.COLUMN_TYPE)
                        .setMode(BeamPipelineConstants.COLUMN_MODE_NULLABLE));
        fieldsList.add(
                new TableFieldSchema()
                        .setName(BeamPipelineConstants.GUID_COLUMN)
                        .setType(BeamPipelineConstants.COLUMN_TYPE)
                        .setMode(BeamPipelineConstants.COLUMN_MODE_REQUIRED));
        fieldsList.add(
                new TableFieldSchema()
                        .setName(BeamPipelineConstants.INSERTION_TIME_STAMP)
                        .setType(BeamPipelineConstants.COLUMN_TYPE_TIMESTAMP)
                        .setMode(BeamPipelineConstants.COLUMN_MODE_REQUIRED));

        return new TableSchema().setFields(fieldsList);
    }
}
