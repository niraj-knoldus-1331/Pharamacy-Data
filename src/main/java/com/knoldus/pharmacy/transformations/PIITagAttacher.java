package com.knoldus.pharmacy.transformations;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.datacatalog.v1.*;
import com.knoldus.pharmacy.models.TableRowSpecs;
import com.knoldus.pharmacy.options.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.knoldus.pharmacy.utils.BeamPipelineConstants.CONTAIN_PII;
import static com.knoldus.pharmacy.utils.BeamPipelineConstants.LINKED_RESOURCE_FORMAT;
import static com.knoldus.pharmacy.utils.Utility.getPIIColumns;

public class PIITagAttacher extends DoFn<KV<TableRow, TableRowSpecs>, KV<TableRow, TableRowSpecs>> {
    final Logger logger = LoggerFactory.getLogger(PubSubMessagetoTableRow.class);

    @ProcessElement
    public void process(ProcessContext processContext, PipelineOptions options) {
        BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);
        KV<TableRow, TableRowSpecs> element = processContext.element();
        TableRowSpecs tableRowSpecs = element.getValue();
        TableRow tableRow = element.getKey();
        String formName = (String) tableRow.get("formName");
        String version = (String) tableRow.get("version");
        List<String> piiColumns = getPIIColumns(bigQueryOptions.getGcsClient(), bigQueryOptions.getSchemaBucket(), formName, version);
        if (piiColumns.size()!=0) {
            TagTemplateName tagTemplateName = TagTemplateName.of(bigQueryOptions.getGcpProject(), bigQueryOptions.getLocation(), bigQueryOptions.getTagTemplate());
            attachTagTemplate(tagTemplateName, piiColumns, bigQueryOptions.getGcpProject(), tableRowSpecs.getDataset(), bigQueryOptions.getTagTemplate().toUpperCase(), tableRowSpecs.getBqTableName());
        }
        processContext.output(KV.of(tableRow, tableRowSpecs));
    }


    public void attachTagTemplate(
            final TagTemplateName tagTemplateName,
            final List<String> columns,
            final String projectId,
            final String datasetId,
            final String displayName,
            final String tableName) {


        try (DataCatalogClient dataCatalogClient = DataCatalogClient.create()) {
            GetTagTemplateRequest request =
                    GetTagTemplateRequest.newBuilder().setName(tagTemplateName.toString()).build();

            TagTemplate tagTemplate = getTagTemplate(dataCatalogClient, request);
            if (tagTemplate == null) {
                tagTemplate = createTag(dataCatalogClient, tagTemplateName, "Contains PII");
            }


            String linkedResource =
                    String.format(LINKED_RESOURCE_FORMAT, projectId, datasetId, tableName);


            LookupEntryRequest lookupEntryRequest = LookupEntryRequest.newBuilder().setLinkedResource(linkedResource).build();

            Entry tableEntry = dataCatalogClient.lookupEntry(lookupEntryRequest);
            DataCatalogClient.ListTagsPagedResponse tagsResponse =
                    dataCatalogClient.listTags(tableEntry.getName());

            HashSet<String> columnNameSet = new HashSet<>();

            for (var tag : tagsResponse.iterateAll()) {
                if (tag.getTemplateDisplayName().equals(displayName)) {
                    columnNameSet.add(tag.getColumn().toLowerCase());
                }
            }

            // -------------------------------
            // Attach a Tag to the table.
            // -------------------------------
            TagField hasPiiValue = TagField.newBuilder().setBoolValue(true).build();

            for (String columnName : columns) {
                if (columnNameSet.contains(columnName.toLowerCase())) {
                    logger.info("Column {} already marked as PII", columnName);
                    continue;
                }
                Tag tag = Tag.newBuilder()
                        .setTemplate(tagTemplate.getName())
                        .putFields(CONTAIN_PII, hasPiiValue)
                        .setColumn(columnName)
                        .build();
                logger.info("Attached PII marker to column {}", columnName);

                dataCatalogClient.createTag(tableEntry.getName(), tag);
            }
        } catch (IOException ioException) {
            logger.error("IO Exception Occurred", ioException);
        } catch (Exception exception) {
            logger.error("Unexpected Exception Occurred", exception);
        }
    }

    private TagTemplate getTagTemplate(
            final DataCatalogClient dataCatalogClient,
            final GetTagTemplateRequest request) {
        try {
            return dataCatalogClient.getTagTemplate(request);
        } catch (Exception exception) {
            logger.error("Template Not Found", exception);
        }
        return null;
    }

    private static TagTemplate createTag(
            final DataCatalogClient dataCatalogClient,
            final TagTemplateName template,
            final String displayName) {

        TagTemplateField hasPiiField =
                TagTemplateField.newBuilder()
                        .setDisplayName(displayName)
                        .setType(FieldType.newBuilder().setPrimitiveType(FieldType.PrimitiveType.BOOL).build())
                        .build();

        var tagTemplate =
                TagTemplate.newBuilder()
                        .setDisplayName(displayName)
                        .putFields(CONTAIN_PII, hasPiiField)
                        .build();

        var createTagTemplateRequest =
                CreateTagTemplateRequest.newBuilder()
                        .setParent(
                                LocationName.newBuilder()
                                        .setProject(template.getProject())
                                        .setLocation(template.getLocation())
                                        .build()
                                        .toString())
                        .setTagTemplateId(template.getTagTemplate())
                        .setTagTemplate(tagTemplate)
                        .build();

        return dataCatalogClient.createTagTemplate(createTagTemplateRequest);
    }
}
