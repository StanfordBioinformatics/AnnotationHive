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
## Section 3: [Variant-based Annotation](./Variant-Annotation.md)
This section explains how to annotate a VCF/mVCF table against any number of variant-based annotation datasets. 
## Section 4: [Interval-based Annotation](./Interval-Annotation.md)
This section explains how to annotate a VCF/mVCF table against any number of interval-based annotation datasets. 
## Section 5: [Variant-based and Interval-based Annotation](./Variant-Interval-Annotation.md)
This section explains how to run a combincation of interval-based and variant-based annotation datasets. 
## Section 6: [Gene-based Annotation](./Gene-Annotation.md)
This section demonstrates how to run our gene-based annotation process for a VCF/mVCF table.
## Section 7: [Sample Experiments](./Experiments.md)
This section provides sevarl experiments on scalability and the cost of the system.

<!---
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
--->

### Annotate Variants Using BigQuery APIs ####


 
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


