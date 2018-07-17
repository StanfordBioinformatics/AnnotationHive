## Section 9: Annotate a Small Number of Variants or Regions
This section explains how to annotate a small number of regions/variants.

* **Input Variants** (variant-based annotations and interval-based annotations)

Users can submit a list of variants as VCF file to AnnotationHive using ```--inputVariant=chr17:40760129:40760129:A:T,chr17:40706906:40706906:G:A,chr17:40751362:40751362:T:C```.

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Google_Cloud_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=<Your_BigQuery_DatasetId>  --outputBigQueryTable=<Output_VCF_Table_Name> --genericAnnotationTables=<ProjectID>:<DatasetID>.<AnnotationTableID>:<Field1>:<Field2>:...:<FieldN> --inputVariant=chr17:40760129:40760129:A:T,chr17:40706906:40706906:G:A,chr17:40751362:40751362:T:C --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/" -Pdataflow-runner
   ```

For our test example: 
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=small_variants_test_chr17 --variantAnnotationTables=<YOUR_Project_ID>:test.sample_variant_annotation_chr17:alleleFreq:dbsnpid --inputVariant=chr17:40760129:40760129:A:T,chr17:40706906:40706906:G:A,chr17:40751362:40751362:T:C --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging" -Pdataflow-runner
   ```
 
* **Input Regions**

Users can also submit a list of variants as VCF file to AnnotationHive using 

   ```--inputRegion=chr17:40751300:40751400,chr17:40755800:40755910```
 

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Google_Cloud_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=<Your_BigQuery_DatasetId>  --outputBigQueryTable=<Output_VCF_Table_Name> --genericAnnotationTables=<ProjectID>:<DatasetID>.<AnnotationTableID>:<Field1>:<Field2>:...:<FieldN> --inputRegion=chr17:40751300:40751400,chr17:40755800:40755910 --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/" -Pdataflow-runner
   ```

For our test example: 
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=samll_regions_transcript_test_chr17 --genericAnnotationTables=<YOUR_Project_ID>:test.sample_transcript_annotation_chr17:name:name2  --inputRegion=chr17:40751300:40751400,chr17:40755800:40755910 --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging" -Pdataflow-runner
   ``` 

