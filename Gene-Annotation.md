## Section 6: Gene-based Annotation
This section demonstrates how to run our gene-based annotation process for a VCF/mVCF table.

There are two functionalities supported by AnnotationHive regarding gene annotation: 1) Finding the closest gene to each varaint, 2) finding all genes overlapped with each varaiant whitin an input proximity threashold.

* Finding the closest gene for each varaint 
   Here are the key options:
   * **--geneBasedAnnotation=true**
   * **--geneBasedMinAnnotation=true**

   ``` 
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --genericAnnotationTables=<Table address Plus selected fields> (e.g., myProject:myPublicAnnotationSets.hg19_refGene:name:name2 - selecting name and name2 from hg19_refGene table) --VCFTables=<VCF_Table_Names>(e.g., genomics-public-data:1000_genomes_phase_3.variants_20150220_release) --outputBigQueryTable=<Output_Table_Name> --geneBasedAnnotation=true --geneBasedMinAnnotation=true --searchRegion=<chromID1:Start1:End1,Chrom2,Start2;...;ChromN:StartN:EndN>" -Pdataflow-runner
   ```
   Here is an exmaple:
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/statging/ --bigQueryDatasetId=test --genericAnnotationTables=<Your_Google_Cloud_Project_Name>:AnnotationHive.hg19_UCSC_refGene:name:name2 --geneBasedAnnotation=true --geneBasedMinAnnotation=true  --outputBigQueryTable=BRCA1_BRAC2_closest_genes_test_chr17 --VCFTables=<Your_Google_Cloud_Project_Name>:test.NA12877_chr17 --searchRegions=chr17:41196311:41277499" -Pdataflow-runner
   ```

<!---   ``` 
 mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<GCP PrjectID> --runner=DataflowRunner --stagingLocation=gs://<Google Bucket>/statging/ --bigQueryDatasetId=test --genericAnnotationTables=<ProjectID>:AnnotationHive.hg19_UCSC_refGene:name:name2 --geneBasedAnnotation=true --geneBasedMinAnnotation=true  --outputBigQueryTable=closest_genes_test_chr17 --VCFTables=<GCP ProjectID>:test.NA12877_chr17" -Pdataflow-runner

   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --numWorkers=64 --gcpTempLocation=gs://<>Your_Google_Cloud_Bucket_Name/<temp DIR> --bigQueryDataset=<YOUR_BigQuery_Dataset_ID> --genericAnnotationTables=<Table address Plus selected fields> (e.g., myProject:myPublicAnnotationSets.hg19_refGene:name:name2 - selecting name and name2 from hg19_refGene table) --VCFTables=<VCF_Table_Names>(e.g., genomics-public-data:1000_genomes_phase_3.variants_20150220_release) --bucketAddrAnnotatedVCF=gs://<Your_Google_Cloud_Bucket_Name>/<annotated_VCF_name>.vcf --workerMachineType=n1-highmem-16 --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --geneBasedAnnotation=true --geneBasedMinAnnotation=true --sampleId=<SAMPLE_ID>" -Pdataflow-runner
   ```


mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<GCP ProjectID> --runner=DataflowRunner --stagingLocation=gs://<Google Storage Bucket>/statging/ --bigQueryDatasetId=test --genericAnnotationTables=<GCP ProjectID>:AnnotationHive.hg19_UCSC_refGene:name:name2 --geneBasedAnnotation=true --proximityThreshold=1000  --outputBigQueryTable=closest_genes_test_1000bp_chr17 --VCFTables=<GCP ProjectID>:test.NA12877_chr17" -Pdataflow-runner

   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --numWorkers=64 --gcpTempLocation=gs://<Your_Google_Cloud_Bucket_Name/<temp DIR> --bigQueryDataset=<YOUR_BigQuery_Dataset_ID> --genericAnnotationTables=<Table address Plus selected fields> (e.g., myProject:myPublicAnnotationSets.hg19_refGene:name:name2 - selecting name and name2 from hg19_refGene table) --VCFTables=<VCF_Table_Names>(e.g., genomics-public-data:1000_genomes_phase_3.variants_20150220_release) --bucketAddrAnnotatedVCF=gs://<Your_Google_Cloud_Bucket_Name>/<annotated_VCF_name>.vcf --workerMachineType=n1-highmem-16 --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --geneBasedAnnotation=true --proximityThreshold=<An_Ineteger_Number> --sampleId=<SAMPLE_ID>" -Pdataflow-runner


--->

* Finding all overlapped genes whitin a specific proximity threashold for each varaint.
   Here are the key options:
   * **--geneBasedAnnotation=true** 
   * **--proximityThreshold=10000**

   ``` 
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --genericAnnotationTables=<Table address Plus selected fields> (e.g., myProject:myPublicAnnotationSets.hg19_refGene:name:name2 - selecting name and name2 from hg19_refGene table) --VCFTables=<VCF_Table_Names>(e.g., genomics-public-data:1000_genomes_phase_3.variants_20150220_release) --outputBigQueryTable=<Output_Table_Name> --geneBasedAnnotation=true --proximityThreshold=<An_Ineteger_Number>" -Pdataflow-runner
  ```

