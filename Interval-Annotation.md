## Section 4: Interval-based Annotation
This section explains how to annotate a VCF/mVCF table against any number of interval-based annotation datasets. 

## mVCF/VCF ##

After successfully importing mVCF/VCF files (refer to [Section 1](Import.md)), you can then run them against any number of
annotation datasets (for information about how to list annotation datasets, refer to [Section 2](List-Annotation.md)).

The following two types of annotation datasets are available: 1) Variant annotation datasets that contain `chrom, start, end, ref, alt, ...`, and 
2) Generic annotation datasets that contain `chrom, start, end, ...`. The main difference is that Generic annotation datasets do not have any 
`ref` or `alt` fields.   

Here are the key parameters for interval-based annotation:

* **--bigQueryDatasetId**: Specify the BigQuery dataset ID that will contain the output annotated VCF table. 
* **--VCFTables**: Specify the BigQuery address of the mVCF/VCF table on BigQuery
* **--VCFCanonicalizeRefNames**: Specify the prefix for the reference field in the VCF tables (e.g, "chr"). AnnotationHive automatically canonicalizes the VCF table by removing the prefix in its calculation. 
* **--projectId**: Specify the project ID that has access to the VCF tables.
* **--bucketAddrAnnotatedVCF**: Specify the full bucket and name address to the output VCF File (e.g., gs://mybucket/outputVCF.vcf).
* **--genericAnnotationTables**: Specify the address of the Generic annotation tables (e.g., gbsc-gcp-project-cba:AnnotationHive.hg19_UCSC_RefGene).
* **--createVCF**: If you want to obtain a VCF file, then set this flag as true (default value is false, and it creates a table).

### AnnotationHive VCF Table ###
To run interval-based annotation, after importing the mVCF/VCF file using AnnotationHive's API, then modify and run the following command:

```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Google_Cloud_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=<Your_BigQuery_DatasetId>  --outputBigQueryTable=<Output_VCF_Table_Name> --genericAnnotationTables=<ProjectID>:<DatasetID>.<AnnotationTableID>:<Field1>:<Field2>:...:<FieldN>  --VCFTables=<ProjectID>:<DatasetID>.<VCF/mVCF_Table_ID> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/" -Pdataflow-runner
```

For our test example: 
```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=annotate_transcript_test_chr17 --genericAnnotationTables=<YOUR_Project_ID>:test.sample_transcript_annotation_chr17:name:name2  --VCFTables=<YOUR_Project_ID>:test.NA12877_chr17 --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging" -Pdataflow-runner
``` 

### Google VCF Table ###
If your VCF table was imported using Google APIs, then set `--googleVCF=true`. Here is a test example for the 1000 Genomes Project mVCF file imported by Google Genomics.

```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<YOUR_Project_ID> --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=annotate_transcript_Google_1000_test_chr17 --genericAnnotationTables=<YOUR_Project_ID>:test.sample_transcript_annotation_chr17:name:name2  --VCFTables=genomics-public-data:1000_genomes_phase_3.variants --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging --googleVCF=true" -Pdataflow-runner
```

Now, if you want AnnotationHive to calculate the number of samples that have variants, then set `--numberSamples=true` . Here is a test example for the 1000 Genomes Project mVCF file imported by Google Genomics (Total number of samples in the mVCF file: 2,504). AnnotationHive will find the number of samples that have variants:

```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=YOUR_Project_ID --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=annotate_transcript_Google_1000_test_chr17_with_num_samples --genericAnnotationTables=<YOUR_Project_ID>:test.sample_transcript_annotation_chr17:name:name2  --VCFTables=genomics-public-data:1000_genomes_phase_3.variants --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging --googleVCF=true --numberSamples=true" -Pdataflow-runner
``` 

