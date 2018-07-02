## Section 3: Variant-based Annotation
This section explains how to annotate a VCF/mVCF table against any number of variant-based annotation datasets. 


## mVCF/VCF ##

After importing mVCF/VCF files successfully (refer to [Section 1](Import.md)), then to run them against a number of
annotation datasets (to list annotation datasets refer to [Section 2](List-Annotation.md)).

Here are the key parameters:

* **--bigQueryDatasetId**: The BigQuery dataset ID that will contain the output annotated VCF table. 
* **--localOutputFilePath**: specify this file when you want to sort the output of BigQuery using BigQuery itself
* **--VCFTables**: The BigQuery address of the mVCF/VCF table on BigQuery
* **--VCFCanonicalizeRefNames**: This provides the prefix for reference field in VCF tables (e.g, "chr"). AnnotationHive automatically canonicalizes the VCF table by removing the prefix in its calculation. 
* **--projectId**: The project ID that has access to the VCF tables.
* **--bucketAddrAnnotatedVCF**: This provides the full bucket and name address to the output VCF File (e.g., gs://mybucket/outputVCF.vcf).
* **--variantAnnotationTables**: This provides the address of variant annotation tables (e.g., gbsc-gcp-project-cba:AnnotationHive.hg19_UCSC_snp144).
* **--createVCF**: If you want to get a VCF file, then set this flag true (default value is false, and it creates a table).

### AnnotationHive VCF Table ###
If you imported mVCF/VCF file using AnnotationHive's API, then modify and run the following command:

```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Google_Cloud_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=<Your_BigQuery_DatasetId>  --outputBigQueryTable=<Output_VCF_Table_Name> --variantAnnotationTables=<ProjectID>:<DatasetID>.<AnnotationTableID>:<Field1>:<Field2>:...:<FieldN>  --VCFTables=<ProjectID>:<DatasetID>.<VCF/mVCF_Table_ID> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/" -Pdataflow-runner
```

For our test example: 
```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=annotate_variant_test_chr17 --variantAnnotationTables=<YOUR_Project_ID>:test.sample_variant_annotation_chr17  --VCFTables=<YOUR_Project_ID>:test.NA12877_chr17 --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging" -Pdataflow-runner
``` 


### Google VCF Table ###
If users have a VCF table imported using Google APIs, and they want to get the number of samples from the multiple VCF file, then they need to set this true. Here is a test example for the 1000 Genomes mVCF file imported by Google Genomics.

```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=annotate_variant_Google_1000_test_chr17 --variantAnnotationTables=<YOUR_Project_ID>:test.sample_variant_annotation_chr17:alleleFreq:dbsnpid  --VCFTables=genomics-public-data:1000_genomes_phase_3.variants --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging --googleVCF=true" -Pdataflow-runner
```

Now, if you want AnnotationHive to calculate the number of samples presenting variants, then set `--numberSamples`. Here is atest example for the 1000 Genomes mVCF file imported by Google Genomics (2,504 samples):

```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=YOUR_Project_ID --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=annotate_variant_Google_1000_test_chr17_with_num_samples --variantAnnotationTables=<YOUR_Project_ID>:test.sample_variant_annotation_chr17:alleleFreq:dbsnpid  --VCFTables=genomics-public-data:1000_genomes_phase_3.variants --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging --googleVCF=true --numberSamples=true" -Pdataflow-runner
``` 

