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

1.1. UploadFile
* Upload the sample transcript (Sample/sample_transcript_annotation_chr17.bed)
  ```
  java -Xbootclasspath/p:alpn-boot.jar   -cp target/cba-v1-jar-with-dependencies.jar   com.google.cloud.genomics.cba.StartAnnotationEngine UploadFile --username=<YOUR_Google_Cloud_Registered_Email> --bucketName=<Your Google CloudBucket Name> --localFilenameAddr=Sample/sample_transcript_annotation_chr17.bed --cloudObjectName=sample_transcript_annotation_chr17.bed
  ```
 
