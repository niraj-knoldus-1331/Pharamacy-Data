//package com.knoldus.pharmacy;
//
//import com.google.cloud.datacatalog.v1.*;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//public class TestCatalog {
//    public static void main(String[] args) {
////        lookupEntry("pharmacydeda","test_de");
//        TagTemplateName name =  TagTemplateName.of("pharmacydeda", "us-central1", "pii");
//        List<String> columns = new ArrayList<>();
//        columns.add("patient.legalMiddleName");
//        attachTagTemplate(name,columns, "pharmacydeda","test_de","PII");
//
//    }
//
//    /**
//     * Lookup the Data Catalog entry referring to a BigQuery Dataset
//     *
//     * @param projectId The project ID to which the Dataset belongs, e.g. 'my-project'
//     * @param datasetId The dataset ID to which the Catalog Entry refers, e.g. 'my_dataset'
//     */
//    public static void lookupEntry(String projectId, String datasetId) {
//        // String projectId = "my-project"
//        // String datasetId = "my_dataset"
//
//        // Get an entry by the resource name from the source Google Cloud Platform service.
//        String linkedResource =
//                String.format("//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s", projectId, datasetId,"medication_review--1_1");
//        LookupEntryRequest request =
//                LookupEntryRequest.newBuilder().setLinkedResource(linkedResource).build();
//
//        // Alternatively, lookup by the SQL name of the entry would have the same result:
//        // String sqlResource = String.format("bigquery.dataset.`%s`.`%s`", projectId, datasetId);
//        // LookupEntryRequest request =
//        // LookupEntryRequest.newBuilder().setSqlResource(sqlResource).build();
//
//        // Initialize client that will be used to send requests. This client only needs to be created
//        // once, and can be reused for multiple requests. After completing all of your requests, call
//        // the "close" method on the client to safely clean up any remaining background resources.
//        try (DataCatalogClient dataCatalogClient = DataCatalogClient.create()) {
//            Entry entry = dataCatalogClient.lookupEntry(request);
//            System.out.printf("Entry name: %s\n", entry);
//
//            DataCatalogClient.ListTagsPagedResponse listTagsPagedResponse = dataCatalogClient.listTags(entry.getName());
//
//
////            for (Tag tag : listTagsPagedResponse.iterateAll()) {
////                System.out.println("Column is " + tag.getColumn());
////            }
////            CreateTagRequest createTagRequest =
////                    CreateTagRequest.newBuilder().setParent(entry.getName()).setField()
//
////            dataCatalogClient.createTag(createTagRequest);
//        }
//        catch(Exception e){
//            System.out.print("Error during lookupEntryBigQueryDataset:\n" + e.toString());
//        }
//    }
//
//    private static TagTemplate getTagTemplate(
//            final DataCatalogClient dataCatalogClient,
//            final GetTagTemplateRequest request) {
//        try {
//            return dataCatalogClient.getTagTemplate(request);
//        } catch (Exception exception) {
////            LOGGER.error("{} - Template Not Found", logUniqueId, exception);
//        }
//        return null;
//    }
//    private static TagTemplate createTag(
//            final DataCatalogClient dataCatalogClient,
//            final TagTemplateName template,
//            final String displayName,
//            final String logUniqueId) {
//
////        LOGGER.debug("{} - creating template tag if not present", logUniqueId);
//        TagTemplateField hasPiiField =
//                TagTemplateField.newBuilder()
//                        .setDisplayName(displayName)
//                        .setType(FieldType.newBuilder().setPrimitiveType(FieldType.PrimitiveType.BOOL).build())
//                        .build();
//
//        var tagTemplate =
//                TagTemplate.newBuilder()
//                        .setDisplayName(displayName)
//                        .putFields(FIELD_NAME, hasPiiField)
//                        .build();
//
//        var createTagTemplateRequest =
//                CreateTagTemplateRequest.newBuilder()
//                        .setParent(
//                                LocationName.newBuilder()
//                                        .setProject(template.getProject())
//                                        .setLocation(template.getLocation())
//                                        .build()
//                                        .toString())
//                        .setTagTemplateId(template.getTagTemplate())
//                        .setTagTemplate(tagTemplate)
//                        .build();
//
//        return dataCatalogClient.createTagTemplate(createTagTemplateRequest);
//    }
//    public static void attachTagTemplate(
//            final TagTemplateName template,
//            final List<String> columns,
//            final String projectId,
//            final String datasetId,
//            final String displayName) {
//
//        // Initialize client that will be used to send requests. This client only needs to be created
//        // once, and can be reused for multiple requests. After completing all of your requests, call
//        // the "close" method on the client to safely clean up any remaining background resources.
//
//        try (var dataCatalogClient = DataCatalogClient.create()) {
//            GetTagTemplateRequest request =
//                    GetTagTemplateRequest.newBuilder().setName(template.toString()).build();
//
//            var tagTemplate = getTagTemplate(dataCatalogClient, request);
//
//            String linkedResource =
//                    String.format("//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s", projectId, datasetId,"medication_review--1_1");
//
//
//
//            var lookupEntryRequest =
//                    LookupEntryRequest.newBuilder().setLinkedResource(linkedResource).build();
//
//            Entry tableEntry = dataCatalogClient.lookupEntry(lookupEntryRequest);
//
//            // -------------------------------
//            // Attach a Tag to the table.
//            // -------------------------------
//            TagField hasPiiValue = TagField.newBuilder().setBoolValue(true).build();
//
//            for (String columnName : columns) {
//
//                var tag =
//                        Tag.newBuilder()
//                                .setTemplate(tagTemplate.getName())
//                                .putFields("contains_pii", hasPiiValue)
//                                .setColumn(columnName)
//                                .build();
//
//                dataCatalogClient.createTag(tableEntry.getName(), tag);
//            }
//        } catch (IOException ioException) {
//            System.out.println(ioException.toString());
//            ioException.printStackTrace();
////            LOGGER.error("{} - IO Exception Occurred {}", logUniqueId, ioException);
//        } catch (Exception exception) {
//            System.out.println(exception.toString());
//            exception.printStackTrace();
////            LOGGER.error("{} - Unexpected Exception Occurred {}", logUniqueId, exception);
//        }
//    }
//
//}
