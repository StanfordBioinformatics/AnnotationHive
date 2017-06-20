# README #

### Cloud-based Annotation Engine ###

* Annotation is the process by which pertinent information about raw DNA sequences is added to genome databases. Multiple software applications have been developed to annotate genetic variants that can be automatically derived from diverse genomes (e.g., ANNOVAR, SnpEff). The first problem using the existing tools is about downloading the software and the large build files. The second problem is scalability. Because current tools are sequential, the annotation of large numbers of patients is tedious and takes a significant amount of time. 

* The pay-as-you-go model of cloud computing, which removes the maintenance effort required for a high performance computing (HPC) facility while simultaneously offering elastic scalability, is well suited for genomic analysis.

* In this project, we developed a cloud-based annotation engine that automatically annotates the user’s input datasets (e.g., VCF, avinput files) in the cloud.

* Version 1.0

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
1. To create and manage datasets click [here](https://cloud.google.com/genomics/v1/managing-datasets)

1. To run:

* Upload the sample transcript annotation (Samples/sample_transcript_annotation_chr17.bed)
 
 ```
  java -Xbootclasspath/p:alpn-boot.jar -cp target/cba-v1-jar-with-dependencies.jar com.google.cloud.genomics.cba.StartAnnotationEngine UploadFile --username=<YOUR_Google_Cloud_Registered_Email> --bucketName=<Your_Google_Cloud_Bucket_Name> --localFilenameAddr=Samples/sample_transcript_annotation_chr17.bed --cloudObjectName=sample_transcript_annotation_chr17.bed
  ```
 
* Upload the sample variant annotation (Samples/sample_variant_annotation_chr17.bed)

 ```
 java -Xbootclasspath/p:alpn-boot.jar -cp target/cba-v1-jar-with-dependencies.jar com.google.cloud.genomics.cba.StartAnnotationEngine UploadFile --username=<YOUR_Google_Cloud_Registered_Email> --bucketName=<Your_Google_Cloud_Bucket_Name> --localFilenameAddr=Samples/sample_variant_annotation_chr17.bed --cloudObjectName=sample_variant_annotation_chr17.bed
``` 

* Upload the sample VCF file (Samples/NA12877-chr17.vcf)

 ```
 java -Xbootclasspath/p:alpn-boot.jar -cp target/cba-v1-jar-with-dependencies.jar com.google.cloud.genomics.cba.StartAnnotationEngine UploadFile --username=<YOUR_Google_Cloud_Registered_Email> --bucketName=<Your_Google_Cloud_Bucket_Name> --localFilenameAddr=Samples/NA12877-chr17.vcf --cloudObjectName=NA12877-chr17.vcf
``` 
* Note: After submitting commands to import VCF and annotation files, make sure to record the "id" value corresponding to each variant or annotation set. These will be needed to submit the "Annotate Variants" job(s) and are not easily gotten, otherwise. If you do need to find them see the following search resources: https://cloud.google.com/genomics/v1beta2/reference/annotationSets/search, https://cloud.google.com/genomics/v1beta2/reference/variantsets/search.

* Import your VCF files into Google Genomics
 ```
 java -Xbootclasspath/p:alpn-boot.jar -cp target/cba-v1-jar-with-dependencies.jar com.google.cloud.genomics.cba.StartAnnotationEngine ImportVCF --datasetId=<Your_Google_GEnomics_DatasetId> --URIs=gs://<YOUR_Google_Bucket_Name>/NA12877-chr17.vcf --variantSetName=NA12877-chr17
``` 

* Note: Before running any of the below dataflow jobs, make sure that your files in the cloud bucket have the reqired access permissions (i.e., cloudservices.gserviceaccount.com, and compute@developer.gserviceaccount.com). Also, make sure the the Genomics API has been enabled in the Google API Manager Dashboard: https://console.developers.google.com/apis/api/genomics/.

* Import annotation files into Google Genomics

 ```
 java -Xbootclasspath/p:alpn-boot.jar -cp target/cba-v1-jar-with-dependencies.jar com.google.cloud.genomics.cba.StartAnnotationEngine ImportAnnotation --datasetId=<Google_Genomics_DatasetId> --annotationSetName=sample_variant_annotation_chr17 --annotationReferenceSetId=EMWV_ZfLxrDY-wE --annotationInputTextBucketAddr=gs://<Your_Google_Cloud_Bucket_Name>/sample_variant_annotation_chr17.bed --runner=DataflowPipelineRunner --project=<Your_Google_Cloud_Project_Name> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --numWorkers=4 --type=VARIANT --header=chrom,chromStart,chromEnd,ref,alterBases,alleleFreq,dbsnpid --base0=no
```

 ```
 java -Xbootclasspath/p:alpn-boot.jar -cp target/cba-v1-jar-with-dependencies.jar com.google.cloud.genomics.cba.StartAnnotationEngine ImportAnnotation --datasetId=<Your_Google_Genomics_DatasetId> --annotationSetName=sample_transcript_annotation_chr17 --annotationReferenceSetId=EMWV_ZfLxrDY-wE --annotationInputTextBucketAddr=gs://<Your_Google_Cloud_Bucket_Name>/sample_transcript_annotation_chr17.bed --runner=DataflowPipelineRunner --project=<Your_Google_Cloud_Project_Name> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --numWorkers=4 --type=Generic --base0=no --header=chrom,txStart,txEnd,bin,name,strand,cdsStart,cdsEnd,exonCount,exonStarts,exonEnds,score,name2,cdsStartStat,cdsEndStat,exonFrames
```

* Annotate Variants

 ```
 java -Xbootclasspath/p:alpn-boot.jar -cp target/cba-v1-jar-with-dependencies.jar com.google.cloud.genomics.cba.StartAnnotationEngine AnnotateVariant --references=chr17:40700000:40800000 --variantSetId=<Your_VariantSetId> --callSetNames=NA12877 --output=gs://<Your_Google_Cloud_Bucket_Name>/dataflow-output/platinum-genomes-Variant-annotation-17.vcf --runner=DataflowPipelineRunner --project=<Your_Google_cloud_Project> --stagingLocation=gs://<Your_Google_Bucket_Name>/<Dataflow-staging_Address> --numWorkers=4 --transcriptSetIds=<Your_Transcript_AnnotationSetId> --variantAnnotationSetIds=<Your_Variant_AnnotationSetId>
```
