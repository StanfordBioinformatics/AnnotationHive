## Section 3: Variant-based Annotation
This section explains how to annotate a VCF/mVCF table against any number of variant-based annotation datasets. 


## mVCF/VCF ##

After successfully importing the mVCF/VCF files (refer to [Section 1](Import.md)), they can be run against a number of
annotation datasets by specifying the following key parameters  (to list annotation datasets, refer to [Section 2](List-Annotation.md)):

* **--bigQueryDatasetId**: Specify the BigQuery dataset ID that will contain the output annotated VCF table. 
* **--VCFTables**: Specify the BigQuery address of the mVCF/VCF table on BigQuery
* **--VCFCanonicalizeRefNames**: Specify the prefix for the reference field in the VCF tables (e.g, "chr"). AnnotationHive automatically canonicalizes the VCF table by removing the prefix in its calculation. 
* **--projectId**: Specify the project ID that has access to the VCF tables.
* **--bucketAddrAnnotatedVCF**: Specify the full bucket and name address to the output VCF file (e.g., gs://mybucket/outputVCF.vcf).
* **--variantAnnotationTables**: Specify the address of the Variant annotation tables (e.g., gbsc-gcp-project-cba:AnnotationHive.hg19_UCSC_snp144).
* **--createVCF**: If you wish to obtain a VCF file, then set this flag true (default value is false, and it creates a table).

### AnnotationHive VCF Table ###
If you imported the mVCF/VCF file using AnnotationHive's API, then modify and run the following command:

```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Google_Cloud_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=<Your_BigQuery_DatasetId>  --outputBigQueryTable=<Output_VCF_Table_Name> --variantAnnotationTables=<ProjectID>:<DatasetID>.<AnnotationTableID>:<Field1>:<Field2>:...:<FieldN>  --VCFTables=<ProjectID>:<DatasetID>.<VCF/mVCF_Table_ID> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/" -Pdataflow-runner
```

For our test example: 
```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=annotate_variant_test_chr17 --variantAnnotationTables=<YOUR_Project_ID>:test.sample_variant_annotation_chr17:alleleFreq:dbsnpid  --VCFTables=<YOUR_Project_ID>:test.NA12877_chr17 --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging" -Pdataflow-runner
``` 

### Google VCF Table ###
If the VCF table was imported using Google APIs, then set `--googleVCF=true`. Here is a test example for the 1000 Genomes Project mVCF file imported by Google Genomics.

```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=annotate_variant_Google_1000_test_chr17 --variantAnnotationTables=<YOUR_Project_ID>:test.sample_variant_annotation_chr17:alleleFreq:dbsnpid  --VCFTables=genomics-public-data:1000_genomes_phase_3.variants --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging --googleVCF=true" -Pdataflow-runner
```

Now, if you want AnnotationHive to calculate the number of samples that have variants, then set `--numberSamples=true`. Here is a test example for the 1000 Genomes Project mVCF file imported by Google Genomics (Total number of samples in the mVCF file: 2,504). AnnotationHive will find the number of samples that have variants:

```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=YOUR_Project_ID --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=annotate_variant_Google_1000_test_chr17_with_num_samples --variantAnnotationTables=<YOUR_Project_ID>:test.sample_variant_annotation_chr17:alleleFreq:dbsnpid  --VCFTables=genomics-public-data:1000_genomes_phase_3.variants --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging --googleVCF=true --numberSamples=true" -Pdataflow-runner
``` 
<!---
If you want to select a sample inside an input mVCF file, you can use the option ```--sampleId```, and set the name of the sampleId (e.g., HG01197 from 1000 genomes project).

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --numWorkers=4  --bigQueryDataset=<YOUR_BigQuery_Dataset_ID> --bigQueryTable=<YOUR_Annotated_VCF_Table_Name> --genericAnnotationTables=<Table address Plus selected fields> (e.g., myProject:myPublicAnnotationSets.hg19_refGene:name:name2 - selecting name and name2 from hg19_refGene table) --VCFTables=<VCF_Table_Names>(e.g., genomics-public-data:1000_genomes_phase_3.variants_20150220_release) --bucketAddrAnnotatedVCF=gs://<Your_Google_Cloud_Bucket_Name>/<annotated_VCF_name>.vcf --workerMachineType=n1-highmem-16 --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --sampleId=<SAMPLE_ID>" -Pdataflow-runner
   ```


--->
