# README #

### AnnotationHive ###

* Annotation is the process by which pertinent information about raw DNA sequences is added to genome databases. Multiple software applications have been developed to annotate genetic variants that can be automatically derived from diverse genomes (e.g., ANNOVAR, SnpEff). The first problem using the existing tools is about downloading the software and the large build files. The second problem is scalability. Because current tools are mainly sequential or parallel only at the node level (requires large machine with many cores and large main memory), the annotation of large numbers of patients is tedious and takes a significant amount of time. 

* The pay-as-you-go model of cloud computing, which removes the maintenance effort required for a high performance computing (HPC) facility while simultaneously offering elastic scalability, is well suited for genomic analysis.

* In this project, we developed a cloud-based annotation engine that annotates input datasets (e.g., VCF, mVCF files) in the cloud using distributed algorithms.

* Version 1.0

## Quickstart

1. Install the [Google Cloud SDK](https://cloud.google.com/sdk/), including the [gcloud tool](https://cloud.google.com/sdk/gcloud/).

1. Setup the gcloud tool.

   ```
   gcloud init
   ```
1. Authentication
   ```
   gcloud auth application-default login
   ```
1. Clone this repo.

   ```
   git clone https://github.com/StanfordBioinformatics/AnnotationHive.git
   ```

1. Install [Maven](http://maven.apache.org/).

<!---
1. [Reference Sets](https://cloud.google.com/genomics/v1/reference-sets) with Google Genomics

   ```
   $ java -jar genomics-tools-client-java-v1beta2.jar searchreferencesets \
     --fields 'referenceSets(id,assemblyId)'
   {"assemblyId":"GRCh37lite","id":"EJjur6DxjIa6KQ"}
   {"assemblyId":"GRCh38","id":"EMud_c37lKPXTQ"}
   {"assemblyId":"hs37d5","id":"EOSt9JOVhp3jkwE"}
   {"assemblyId":"GRCh37","id":"EOSsjdnTicvzwAE"}
   {"assemblyId":"hg19","id":"EMWV_ZfLxrDY-wE"}
   ``` 
1. To create and manage Google Genomics datasets click [here](https://cloud.google.com/genomics/)
--->

## Section 1: [Import VCF/mVCF/Annotation Files](./Import.md)
This section explains how to import VCF, mVCF and annotation files to BigQuery.
## Section 2: [List Available Public Annotation Datasets](./List-Annotation.md)
This part of the code demosntrates how to list AnnotationHive's public datasets.
## Section 3: [Variant-based Annotation](./VariantAnnotation.md)
This section explains how to annotate a VCF/mVCF table against any number of variant-based annotation datasets. 
## Section 4: [Interval-based Annotation](./IntervalAnnotation.md)
This section explains how to annotate a VCF/mVCF table against any number of interval-based annotation datasets. 
## Section 5: [Variant-based and Interval-based Annotation](./VaraintIntervalAnnotation.md)
This section explains how to run a combincation of interval-based and variant-based annotation datasets. 
## Section 6: [Gene-based Annotation](./GeneAnnotation.md)
This section demonstrates how to run our gene-based annotation process for a VCF/mVCF table.
## Section 7: [Sample Experiments](./Experiments.md)
This section provides sevarl experiments on scalability and the cost of the system.


1. To run:

### Upload ###

* Upload the sample transcript annotation (Samples/sample_transcript_annotation_chr17.bed)
 
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="UploadFileToGCS --project=<YOUR_Google_Cloud_Project_ID> --username=<YOUR_Google_Cloud_Registered_Email> --bucketName=<Your_Google_Cloud_Bucket_Name> --localFilenameAddr=Samples/sample_transcript_annotation_chr17.bed --cloudObjectName=sample_transcript_annotation_chr17.bed --runner=DataflowRunner" -Pdataflow-runner
   ```

* Alternatively, you can easily upload/remove/read/rename files on the Google Cloud Storage using [gsutil] (https://cloud.google.com/storage/docs/gsutil) tool 

   ```
   gsutil cp Samples/sample_transcript_annotation_chr17.bed gs://<Your_Google_Cloud_Bucket_Name>/<DIR>
   ```

* Upload the sample variant annotation (Samples/sample_variant_annotation_chr17.bed)

   ```
   gsutil cp Samples/sample_variant_annotation_chr17.bed gs://<Your_Google_Cloud_Bucket_Name>/<DIR>
   ``` 

* Upload the sample VCF file (Samples/NA12877-chr17.vcf)

   ```
   gsutil cp Samples/NA12877-chr17.vcf gs://<Your_Google_Cloud_Bucket_Name>/<DIR>
   ``` 
### Import ###

* Note: After submitting the following command for importing VCF and annotation files, make sure to record the "id" value corresponding to each variant or annotation set. These will be needed to submit the "Annotate Variants" job(s) and are not easily gotten, otherwise. If you do need to find them see the following search resources: https://cloud.google.com/genomics/v1beta2/reference/annotationSets/search, https://cloud.google.com/genomics/v1beta2/reference/variantsets/search.

* Import your VCF files into Google Genomics

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportVCFFromGCSToGG --datasetId=<Your_Google_GEnomics_DatasetId> --URIs=gs://<YOUR_Google_Bucket_Name>/NA12877-chr17.vcf --variantSetName=NA12877-chr17 --runner=DataflowRunner" -Pdataflow-runner
   ``` 

* Note: Before running any of the below dataflow jobs, make sure that your files in the cloud bucket have the reqired access permissions (i.e., cloudservices.gserviceaccount.com, and compute@developer.gserviceaccount.com). Also, make sure the the Genomics API has been enabled in the Google API Manager Dashboard: https://console.developers.google.com/apis/api/genomics/.

* Import annotation files into Google Genomics

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportAnnotationFromGCSToGG --datasetId=<Google_Genomics_DatasetId> --annotationSetName=sample_variant_annotation_chr17 --annotationReferenceSetId=EMWV_ZfLxrDY-wE --annotationInputTextBucketAddr=gs://<Your_Google_Cloud_Bucket_Name>/sample_variant_annotation_chr17.bed --runner=DataflowRunner --project=<Your_Google_Cloud_Project_Name> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --numWorkers=4 --type=VARIANT --header=chrom,chromStart,chromEnd,ref,alterBases,alleleFreq,dbsnpid --base0=no" -Pdataflow-runner
   ```

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportAnnotationFromGCSToGG --datasetId=<Google_Genomics_DatasetId> --annotationSetName=sample_transcript_annotation_chr17.bed --annotationReferenceSetId=EMWV_ZfLxrDY-wE --annotationInputTextBucketAddr=gs://<Your_Google_Cloud_Bucket_Name>/sample_transcript_annotation_chr17.bed --runner=DataflowRunner --project=<Your_Google_Cloud_Project_Name> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --numWorkers=4 --type=Generic --base0=no --header=chrom,txStart,txEnd,bin,name,strand,cdsStart,cdsEnd,exonCount,exonStarts,exonEnds,score,name2,cdsStartStat,cdsEndStat,exonFrames" -Pdataflow-runner
   ```

### Annotate Variants Using Google Genomics (GG) APIs ###

* Local Sort (AnnotationHive will print local sort instructions at the end of the execution)

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="GGAnnotateVariants --references=chr17:40700000:40800000 --variantSetId=<Your_VariantSetId> --callSetNames=NA12877 --output=gs://<Your_Google_Cloud_Bucket_Name>/dataflow-output/platinum-genomes-Variant-annotation-17.vcf --runner=DataflowRunner --project=<Your_Google_cloud_Project> --stagingLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --numWorkers=4 --transcriptSetIds=<Your_Transcript_AnnotationSetId> --variantAnnotationSetIds=<Your_Variant_AnnotationSetId>" -Pdataflow-runner
   ```

* BigQuery Sort
    ```--bigQuerySort```
    ```--bigQueryDatasetId``` 
    ```--bigQueryTable```
    ```--localOutputFilePath```
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="GGAnnotateVariants --references=chr17:40700000:40800000 --variantSetId=<Your_VariantSetId> --callSetNames=NA12877 --output=gs://<Your_Google_Cloud_Bucket_Name>/dataflow-output/platinum-genomes-Variant-annotation-17.vcf --runner=DataflowRunner --project=<Your_Google_cloud_Project> --stagingLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --numWorkers=4 --transcriptSetIds=<Your_Transcript_AnnotationSetId> --variantAnnotationSetIds=<Your_Variant_AnnotationSetId> --bigQuerySort=true --bigQueryDatasetId=<BigQuery_Dataset_Name> --bigQueryTable=<Sample_Output_Table> --localOutputFilePath=<Local_Output_Annotated_VCF_File_Address>" -Pdataflow-runner
   ```

* Exporting Google Genomics variant set to BigQuery

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ExportVCFFromGGToBigQuery --project=<Your_Google_cloud_Project> --stagingLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address>  --bigQueryTableId=<BigQuery_Table_ID> --variantSetId=<Your_Google_cloud_Project> --bigQueryDataSetId=<BigQuery_Google_Dataset_ID>"
   ```

### Annotate Variants Using BigQuery APIs ####

* Import Annotation files to BigQuery
Make sure to run the following command first to create a table for tracking annotation sets inside your BigQuery Dataset; the following command creates a table inside your BigQuery dataset called "AnnotationList".  

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<Your_Google_cloud_Project> --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --runner=DataflowRunner " -Pdataflow-runner
   ```


After creating AnnotationList Table successfully, then you can import your annotation sets from Google Storage:

   ```--annotationType```
   ```--bigQueryDatasetId```
   ```--annotationInputTextBucketAddr```
   ```--annotationSetInfo```
   ```--assemblyId```
   ```--base0```
   ```--bigQueryAnnotationSetTableId```
   ```--header```
   ```--annotationType```
   ```--annotationSetVersion```

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportAnnotationFromGCSToBigQuery --project=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --numWorkers=4 --annotationInputTextBucketAddr=gs://<Your_Google_Cloud_Bucket_Name>/sample_variant_annotation_chr17.bed --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --annotationType=variant --header=chrom,chromStart,chromEnd,ref,alterBases,alleleFreq,dbsnpid --base0=no --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --bigQueryAnnotationSetTableId=sample_variant_annotation_chr17.bed --annotationSetVersion=1.0 --assemblyId=hg19 --annotationSetInfo='Link=..., Date=DD-MM-YYYY'" -Pdataflow-runner
   ```

   ```--columnSeparator```
You can specify sepatator character between columns (default value is '\\s+' that covers all type of spaces. If you only have tab then set this option to `\t`)   

   ```--forceUpdate```
If your table (i.e., bigQueryAnnotationSetTableId) exists, and you want to update it, set the following option true, then it will remove the existing table and import the new version.

 
There are two options for sorting the annotated VCF file: 1) DataFlow Sort and 2) BigQuery Sort. 
If you have a small input file (e.g., one sample VCF file), we recommend you use the BigQury Sort solution. However, if you have a mVCF file or many annotation reference files involved in the annotation process, then we recommend to use Dataflow Sort solution (Documentation: under development). 

* Dataflow Sort
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --numWorkers=4  --bigQueryDataset=<YOUR_BigQuery_Dataset_ID> --bigQueryTable=<YOUR_Annotated_VCF_Table_Name> --genericAnnotationTables=<Table address Plus selected fields> (e.g., myProject:myPublicAnnotationSets.hg19_refGene:name:name2 - selecting name and name2 from hg19_refGene table) --VCFTables=<VCF_Table_Names>(e.g., genomics-public-data:1000_genomes_phase_3.variants_20150220_release) --bucketAddrAnnotatedVCF=gs://<Your_Google_Cloud_Bucket_Name>/<annotated_VCF_name>.vcf --workerMachineType=n1-highmem-16 --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address>" -Pdataflow-runner
   ```

* BigQuery Sort
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --numWorkers=4 --gcpTempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --bigQueryDataset=<YOUR_BigQuery_Dataset_Name>  --bigQueryTable=<The_Output_Table_Name> --variantAnnotationTables=<Table address Plus selected fields> (e.g., myProject:myPublicAnnotationSets.hg19_refGene:name:name2 - selecting name and name2 from hg19_refGene table) --VCFTables=<VCF_Table_Names> --output=gs://<Your_Google_Cloud_Bucket_Name>/<annotated_VCF_name>.vcf --workerMachineType=n1-standard-16 --bigQuerySort=true --localOutputFilePath=<Local_Annotated_VCF_File_Address>" -Pdataflow-runner
   ```

If you want to select a sample inside an input mVCF file, you can use the option ```--sampleId```, and set the name of the sampleId (e.g., HG01197 from 1000 genomes project).

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --numWorkers=4  --bigQueryDataset=<YOUR_BigQuery_Dataset_ID> --bigQueryTable=<YOUR_Annotated_VCF_Table_Name> --genericAnnotationTables=<Table address Plus selected fields> (e.g., myProject:myPublicAnnotationSets.hg19_refGene:name:name2 - selecting name and name2 from hg19_refGene table) --VCFTables=<VCF_Table_Names>(e.g., genomics-public-data:1000_genomes_phase_3.variants_20150220_release) --bucketAddrAnnotatedVCF=gs://<Your_Google_Cloud_Bucket_Name>/<annotated_VCF_name>.vcf --workerMachineType=n1-highmem-16 --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --sampleId=<SAMPLE_ID>" -Pdataflow-runner
   ```
* Storing the annotated VCF file as a BigQuery Table (In this case, AnnotationHive bypasses the sorting phase)
If you would like to store the annotated VCF file as a table so that you can run other queries on your table, you can specify ```outputBigQueryTable```.

* Gene-based Annotation 
There are two functionalities supported by AnnotationHive regarding gene annotation: 1) Finding the closest gene to each varaint, 2) finding all genes overlapped with each varaiant whitin an input proximity threashold.

* Finding the closest gene for each varaint [```--geneBasedAnnotation=true```, ```--geneBasedMinAnnotation=true```]

   ``` 
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --numWorkers=64 --gcpTempLocation=gs://<>Your_Google_Cloud_Bucket_Name/<temp DIR> --bigQueryDataset=<YOUR_BigQuery_Dataset_ID> --genericAnnotationTables=<Table address Plus selected fields> (e.g., myProject:myPublicAnnotationSets.hg19_refGene:name:name2 - selecting name and name2 from hg19_refGene table) --VCFTables=<VCF_Table_Names>(e.g., genomics-public-data:1000_genomes_phase_3.variants_20150220_release) --bucketAddrAnnotatedVCF=gs://<Your_Google_Cloud_Bucket_Name>/<annotated_VCF_name>.vcf --workerMachineType=n1-highmem-16 --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --geneBasedAnnotation=true --geneBasedMinAnnotation=true --sampleId=<SAMPLE_ID>" -Pdataflow-runner
   ```

* Finding all overlapped genes whitin a specific proximity threashold for each varaint [```--geneBasedAnnotation=true```, ```--proximityThreshold=10000```]

   ``` 
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --numWorkers=64 --gcpTempLocation=gs://<Your_Google_Cloud_Bucket_Name/<temp DIR> --bigQueryDataset=<YOUR_BigQuery_Dataset_ID> --genericAnnotationTables=<Table address Plus selected fields> (e.g., myProject:myPublicAnnotationSets.hg19_refGene:name:name2 - selecting name and name2 from hg19_refGene table) --VCFTables=<VCF_Table_Names>(e.g., genomics-public-data:1000_genomes_phase_3.variants_20150220_release) --bucketAddrAnnotatedVCF=gs://<Your_Google_Cloud_Bucket_Name>/<annotated_VCF_name>.vcf --workerMachineType=n1-highmem-16 --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --geneBasedAnnotation=true --proximityThreshold=<An_Ineteger_Number> --sampleId=<SAMPLE_ID>" -Pdataflow-runner
  ```

* Input Varinats (Filter-based annotations and region-based annotations)

Users can submit a list of variants as VCF file to AnnotationHive using ```--inputVariant=chr11:25900005:25900005:C:A,chr11:25900002:25900002:C:A,chrY:9323748:9323748:A:G```.

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --gcpTempLocation=gcpTempLocation=gs://<Your_Google_Cloud_Bucket_Name/<temp DIR> --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID>  --outputBigQueryTable=<YOUR_Output_Table> --variantAnnotationTables=<>  --inputVariant=chr11:25900005:25900005:C:A,chr11:25900002:25900002:C:A,chrY:9323748:9323748:A:G --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --localOutputFilePath=<Local_Dir>/YOUR_filename.vcf --bigQuerySort=true" -Pdataflow-runner
   ```

* Input Regions

Users can also submit a list of variants as VCF file to AnnotationHive using 

   ```--inputRegion=chr11:25900005:25900505,chrY:9323748:9323848```
 
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotateVariants --projectId=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --gcpTempLocation=gcpTempLocation=gs://<Your_Google_Cloud_Bucket_Name/<temp DIR> --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID>  --outputBigQueryTable=<YOUR_Output_Table> --variantAnnotationTables=<>  --inputRegion=chr11:25900005:25900405,chrY:9323748:9323848 --tempLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --localOutputFilePath=<Local_Dir>/YOUR_filename.vcf --bigQuerySort=true" -Pdataflow-runner
   ```

# Experiments #

### AnnotationHive vs. Annovar ###

In the following experiment, we tested AnnotationHive and 
Annovar for one sample (HG00096) of 1000 genomes with over 4.2M 
variants, and for all 1000 samples with over 85.2M variants against 
the following five annotation datasets:

![Annotation Datasets](https://github.com/StanfordBioinformatics/cloud-based-annotation/blob/master/common/img/Annotations.png "Annotation Datasets")


---

* Execution Time

Over 16B annotation records were processed. The y-axis is logarithmic and represents the execution time in minutes. 
The number of variants is depicted on the x-axis. In both cases, AnnotationHive is around two orders of magnitude faster 
than Annovar. For this experiment, we used n1-highmem-16 instances for Annovar and AnnotationHive's Dataflow sort function. 

![AnnotationHive vs. Annovar](https://github.com/StanfordBioinformatics/cloud-based-annotation/blob/master/common/img/Experiment_AnnotationHive_BigQuery.png "AnnotationHive vs. Annovar")

---

* Accuracy

We compared the annotated VCF files for the BRCA1 region. All records are the same except three records with 
genotype values of 0 where Annovar considered them in the output. We filter out variants with every genotype
value less than or equal 0.
 


