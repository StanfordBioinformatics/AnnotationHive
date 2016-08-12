# README #

### Cloud-based Annotation Engine ###

* Annotation is the process by which pertinent information about raw DNA sequences is added to genome databases. Multiple software applications have been developed to annotate genetic variants that can be automatically derived  from diverse genomes (e.g., ANNOVAR, SnpEff). The first problem using the existing tools is about downloading the software and the large build files. The second problem is scalability. Because current tools are sequential, the annotation of large numbers of patients is tedious and takes a significant amount of time. 

* The pay-as-you-go model of cloud computing, which removes the maintenance effort required for a high performance computing (HPC) facility while simultaneously offering elastic scalability, is well suited for genomic analysis.

* In this project, we developed a cloud-based annotation engine that automatically annotates the user’s input datasets (e.g., VCF, avinput files) in the cloud.

* Version 1.0

### How do I get set up? ###

* Install Apache Maven 3.3.3 
* cd cloud-based-annotation/java
* mvn javadoc:javadoc
* mvn assembly:assembly
* java -jar target/cba-v1-jar-with-dependencies.jar <Cloud Project ID> <ReferenceSetId> <DatasetId> <Google Dataflow Staging Path (e.g., gs://myBucketName/staging)> <Address of Input Object (e.g., gs://myBucketName/myObject.txt)> <Address of Output Object>


## Quickstart

1. Install the [Google Cloud SDK](https://cloud.google.com/sdk/), including the [gcloud tool](https://cloud.google.com/sdk/gcloud/).

1. Setup the gcloud tool.

   ```
   gcloud init
   ```

1. Clone this repo.

   ```
   git clone https://github.com/StanfordBioinformatics/cloud-based-annotation.git
   ```

1. Install [Maven](http://maven.apache.org/).

1. Build this project:

   ```
   mvn clean compile assembly:single
   ```
1. To run:

* Upload the sample transcript (Sample/sample_transcript_annotation_chr17.bed)
 
 ```
  java -Xbootclasspath/p:alpn-boot.jar   -cp target/cba-v1-jar-with-dependencies.jar   com.google.cloud.genomics.cba.StartAnnotationEngine UploadFile --username=<YOUR_Google_Cloud_Registered_Email> --bucketName=<Your_Google_Cloud_Bucket_Name> --localFilenameAddr=Sample/sample_transcript_annotation_chr17.bed --cloudObjectName=sample_transcript_annotation_chr17.bed
  ```
 
* Upload the sample variant annotation (Sample/sample_variant_annotation_chr17.bed)

 ```
 java -Xbootclasspath/p:alpn-boot.jar   -cp target/cba-v1-jar-with-dependencies.jar   com.google.cloud.genomics.cba.StartAnnotationEngine UploadFile --username=<YOUR_Google_Cloud_Registered_Email> --bucketName=<Your_Google_Cloud_Bucket_Name> --localFilenameAddr=Sample/sample_variant_annotation_chr17.bed --cloudObjectName=sample_variant_annotation_chr17.bed
``` 

* Upload the sample VCF file (Sample/NA12877-chr17.vcf)

 ```
 java -Xbootclasspath/p:alpn-boot.jar   -cp target/cba-v1-jar-with-dependencies.jar   com.google.cloud.genomics.cba.StartAnnotationEngine UploadFile --username=<YOUR_Google_Cloud_Registered_Email> --bucketName=<Your_Google_Cloud_Bucket_Name> --localFilenameAddr=Sample/NA12877-chr17.vcf --cloudObjectName=NA12877-chr17.vcf
``` 

* Import your VCF files into Google Genomics
 ```
 java -Xbootclasspath/p:alpn-boot.jar   -cp target/cba-v1-jar-with-dependencies.jar   com.google.cloud.genomics.cba.StartAnnotationEngine ImportVCF --datasetId=<Your_Google_GEnomics_DatasetId> --URIs=gs://<YOUR_Google_Bucket_Name>/NA12877-chr17.vcf --variantSetName=NA12877-chr17
``` 

* Import annotation files

 ```
 java -Xbootclasspath/p:alpn-boot.jar -cp target/cba-v1-jar-with-dependencies.jar com.google.cloud.genomics.cba.StartAnnotationEngine ImportAnnotation --datasetId=<Google_Genomics_DatasetId> --annotationSetName=sample_variant_annotation_chr17  --annotationReferenceSetId=EMWV_ZfLxrDY-wE --annotationInputTextBucketAddr=gs://gbsc-gcp-project-cba-user-abahman/annotation/sample_variant_annotation_chr17.bed   --runner=DataflowPipelineRunner --project=<Your_Google_Cloud_Project_Name> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --numWorkers=4 --type=VARIANT --header=chrom,chromStart,chromEnd,ref,alterBases,alleleFreq,dbsnpid --base0=no
```

 ```
 java -Xbootclasspath/p:alpn-boot.jar -cp target/cba-v1-jar-with-dependencies.jar com.google.cloud.genomics.cba.StartAnnotationEngine ImportAnnotation  --datasetId=<Your_Google_Genomics_DatasetId> --annotationSetName=sample_transcript_annotation_chr17 --annotationReferenceSetId=EMWV_ZfLxrDY-wE --annotationInputTextBucketAddr=gs://<Your_Google_Cloud_Bucket_Name>/sample_transcript_annotation_chr17.bed  --runner=DataflowPipelineRunner --project=gbsc-gcp-project-cba --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --numWorkers=4 --type=Generic --base0=no --header=chrom,txStart,txEnd,bin,name,strand,cdsStart,cdsEnd,exonCount,exonStarts,exonEnds,score,name2,cdsStartStat,cdsEndStat,exonFrames
```
