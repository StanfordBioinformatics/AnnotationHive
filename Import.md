## Section 1: Import VCF/mVCF/Annotation Files
This section explains how to import VCF, mVCF and annotation files to BigQuery. AnnotationHive uses Google Dataflow to import Annotation and VCF/mVCF files
to BigQuery.


### Upload ###
The first step in importing VCF/annotation files is to upload your local files to a bucket in Google.


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

### Import mVCF/VCF ###

There are two ways to import VCF files from Google Storage to Google BigQuery: 1) Using Google Genomics API, 2) Using AnnotationHive's API. In order to calaulte number of samples with a genotype larger than 0, the mVCF file must be in 
the Google Genomics format. So, if you have a mVCF file, and want AnnotationHive to calcaule the number of samples, please use Google Genomics solution.

<!--- * Note: After submitting the following command for importing VCF and annotation files, make sure to record the "id" value corresponding to each variant or annotation set. These will be needed to submit the "Annotate Variants" job(s) and are not easily gotten, otherwise. If you do need to find them see the following search resources: https://cloud.google.com/genomics/v1beta2/reference/annotationSets/search, https://cloud.google.com/genomics/v1beta2/reference/variantsets/search. --->

* Import mVCF/VCF files using the API provided by Google Genomics ([More Info](https://cloud.google.com/genomics/docs/how-tos/load-variants))
* Import mVCF/VCF files using the API provided by AnnotationHives 
If this is the first time, you want to import a VCF file using AnnotationHive, please run the following command. This command will create a new table called VCFList that stores metadata and keeps track of your VCF files.

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportVCFFromGCSToBigQuery --project=<Your_Google_Cloud_Project_Name> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/  --bigQueryDatasetId=<Google_Genomics_DatasetId> --runner=DataflowRunner --createVCFListTable=true" -Pdataflow-runner
   ```


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

