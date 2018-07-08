## Section 9: Annotate a Small Number of Variants or Regions
This section explains how to annotate a small number of regions/variants.

* **Input Varinats** (variant-based annotations and interval-based annotations)

Users can submit a list of variants as VCF file to AnnotationHive using ```--inputVariant=chr11:25900005:25900005:C:A,chr11:25900002:25900002:C:A,chrY:9323748:9323748:A:G```.

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --gcpTempLocation=gcpTempLocation=gs://<Your_Google_Cloud_Bucket_Name/<temp DIR> --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID>  --outputBigQueryTable=<YOUR_Output_Table> --variantAnnotationTables=<>  --inputVariant=chr11:25900005:25900005:C:A,chr11:25900002:25900002:C:A,chrY:9323748:9323748:A:G --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --localOutputFilePath=<Local_Dir>/YOUR_filename.vcf --bigQuerySort=true" -Pdataflow-runner
   ```

* **Input Regions**

Users can also submit a list of variants as VCF file to AnnotationHive using 

   ```--inputRegion=chr11:25900005:25900505,chrY:9323748:9323848```
 
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --gcpTempLocation=gcpTempLocation=gs://<Your_Google_Cloud_Bucket_Name/<temp DIR> --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID>  --outputBigQueryTable=<YOUR_Output_Table> --variantAnnotationTables=<>  --inputRegion=chr11:25900005:25900405,chrY:9323748:9323848 --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --localOutputFilePath=<Local_Dir>/YOUR_filename.vcf --bigQuerySort=true" -Pdataflow-runner
   ```

