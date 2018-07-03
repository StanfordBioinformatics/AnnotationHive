## Section 8: Export Annotated VCF Table
This section explains how to export an annotated VCF file. Since AnnotationHive by default stores output files in BigQuery, the output is unsorted. To export an annotated VCF file, we have to sort the table and then store it as a VCF file. 

There are two options for sorting the annotated VCF file: 1) DataFlow Sort and 2) BigQuery Sort. 
If you have a small input file (e.g., one exome VCF file), we recommend you use the BigQury Sort solution. However, if you have a mVCF file or many annotation reference files involved in the annotation process, then we recommend to use Dataflow Sort solution. 

* Dataflow Sort
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --bigQueryDatasetId=test  --outputBigQueryTable=annotate_variant_Google_1000_test_chr17_with_num_samples_Export_WO_Sample --variantAnnotationTables=<Your_Google_Cloud_Project_Name>:test.sample_variant_annotation_chr17:alleleFreq:dbsnpid  --VCFTables=genomics-public-data:1000_genomes_phase_3.variants --stagingLocation=gs://<Your_Google_Bucket_Name>/staging --googleVCF=true --workerMachineType=n1-highmem-16 --bucketAddrAnnotatedVCF=gs://<Your_Google_Cloud_Bucket_Name>/Output.VCF --createVCF=true" -Pdataflow-runner
   ```
   ```
   INFO: ------------------------------------------------------------------------
Header: 

 Chrom	Start	End	Ref	Alt	<sample_variant_annotation_chr17(alleleFreq/dbsnpid),1>

INFO: To download the annotated VCF file from Google Cloud, run the following command:
INFO: 	 ~: gsutil cat gs://<Your_Google_Cloud_Project_Name>/Output.VCF* > Output.VCF

INFO: To remove the output files from the cloud storage run the following command:
INFO: 	 ~: gsutil rm gs://<Your_Google_Cloud_Project_Name>/OutputWOSample.VCF* 
INFO: ------------------------------------------------------------------------
   ```



* BigQuery Sort
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --bigQueryDataset=<YOUR_BigQuery_Dataset_Name> --outputBigQueryTable=<The_Output_Table_Name> --variantAnnotationTables=<Table address Plus selected fields> (e.g., myProject:myPublicAnnotationSets.hg19_refGene:name:name2 - selecting name and name2 from hg19_refGene table) --VCFTables=<VCF_Table_Names> --output=gs://<Your_Google_Cloud_Bucket_Name>/<annotated_VCF_name>.vcf --bigQuerySort=true --localOutputFilePath=<Local_Annotated_VCF_File_Address> --googleVCF=true --numberSamples=true" -Pdataflow-runner
   ```
Here is a test example for the 1000 Genomes project: 
```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --bigQueryDatasetId=test --outputBigQueryTable=annotate_variant_Google_1000_test_chr17_with_num_samples_Export --variantAnnotationTables=<Your_Google_Cloud_Project_Name>:test.sample_variant_annotation_chr17:alleleFreq:dbsnpid  --VCFTables=genomics-public-data:1000_genomes_phase_3.variants --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/staging --googleVCF=true --numberSamples=true --bigQuerySort=true --localOutputFilePath=<Local_Address>/Output.vcf --createVCF=true" -Pdataflow-runner
```

* **--localOutputFilePath**: specify this file when you want to sort the output of BigQuery using BigQuery itself
