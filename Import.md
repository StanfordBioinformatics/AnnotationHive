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

* Import mVCF/VCF files using the API provided by Google Genomics (Please follow instructions provided by [this link](https://cloud.google.com/genomics/docs/how-tos/load-variants))
* Import mVCF/VCF files using the API provided by AnnotationHive 
If this is the first time, you plan to import a VCF file using AnnotationHive, please run the following command. This command will create a new table called VCFList that stores metadata and keeps track of your VCF files.

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportVCFFromGCSToBigQuery --project=<Your_Google_Cloud_Project_Name> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/  --bigQueryDatasetId=<Google_Genomics_DatasetId> --runner=DataflowRunner --createVCFListTable=true" -Pdataflow-runner
   ```

Use the following command to import mVCF/VCF files
```
mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportVCFFromGCSToBigQuery --project=<Your_Google_Cloud_Project_Name> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/  --bigQueryDatasetId=<Google_Genomics_DatasetId> --header=CHROM,POS,ID,REF,ALT,QUAL,FILTER,INFO,FORMAT,NA12877 --columnOrder=1,2,2,4,5 --base0=no --bigQueryVCFTableId=NA12877_chr17 --VCFInputTextBucketAddr=gs://<YOUR_Google_Bucket_Name>/NA12877-chr17.vcf --VCFVersion=1.0 --assemblyId=hg19 --columnSeparator=\t --POS=true  --runner=DataflowRunner" -Pdataflow-runner
``` 

### Import annotation files into BigQuery ###

In order to import annotation datasets to BigQuery, first run the following command to create a table for tracking annotation sets inside your BigQuery Dataset called "AnnotationList".  

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<Your_Google_cloud_Project> --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --createAnnotationSetListTable=true --runner=DataflowRunner " -Pdataflow-runner
   ```

After creating AnnotationList Table successfully, then you can import your annotation sets from Google Storage. Here are the main options:

* **--annotationType**: This provides whether the annotation is Varaiant or Generic annotation dataset. This is a required field. 
* **--bigQueryDatasetId**: This provides a BigQuery dataset ID 
* **--annotationInputTextBucketAddr**: This provides the URI of inputfile which contains annotations. This is a required field.
* **--annotationSetInfo**: This provides more info about the annotationset (e.g., Source='http://hgdownload.soe.ucsc.edu/goldenPath/hg19/database/ensGene.txt.gz'). This is an optional filed.
* **--assemblyId**: This provides assemblyId (e.g., "hg19"). This is a required field.
* **--base0**: This provides whether the base is 0 or 1. This is a required field. 
* **--bigQueryAnnotationSetTableId**: This provides the name of the BigQuery Table for the new annotation dataset. 
* **--header**: This provides the header for the Annotations. This is a required field (e.g., "Chrom,start,end,ref,alter,all other fileds"). 
* **--annotationSetVersion**: This provides the version annotationset (e.g., v1.0). This is a required field. 
* **--columnOrder**: In case the input file does not follow (chrom ID, start, end, ...) for generic annotation, and (chrom ID, start, end, ref, alt, ...) for Variant annotation, then you can specify the order using this option. For example, if `header` is `bin,chrom,info1,start,end,infor2` then `--columnOrder=2,4,5`. AnnotationHive automatically, reorder the fileds.    
* **--columnSeparator**: You can specify sepatator character between columns (default value is '\\s+' that covers all type of spaces.`Note`, if you only have tab, then set this option to `\t`).
* **--forceUpdate**:If your table (i.e., bigQueryAnnotationSetTableId) exists, and you want to update it, set the following option true, then it will remove the existing table and import the new version.



   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportAnnotationFromGCSToBigQuery --project=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --numWorkers=4 --annotationInputTextBucketAddr=gs://<Your_Google_Cloud_Bucket_Name>/sample_variant_annotation_chr17.bed --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --annotationType=variant --header=chrom,chromStart,chromEnd,ref,alterBases,alleleFreq,dbsnpid --base0=no --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --bigQueryAnnotationSetTableId=sample_variant_annotation_chr17.bed --annotationSetVersion=1.0 --assemblyId=hg19 --annotationSetInfo='Link=..., Date=DD-MM-YYYY'" -Pdataflow-runner
   ```

<!--- * Note: After submitting the following command for importing VCF and annotation files, make sure to record the "id" value corresponding to each variant or annotation set. These will be needed to submit the "Annotate Variants" job(s) and are not easily gotten, otherwise. If you do need to find them see the following search resources: https://cloud.google.com/genomics/v1beta2/reference/annotationSets/search, https://cloud.google.com/genomics/v1beta2/reference/variantsets/search.

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportVCFFromGCSToGG --datasetId=<Your_Google_GEnomics_DatasetId> --URIs=gs://<YOUR_Google_Bucket_Name>/NA12877-chr17.vcf --variantSetName=NA12877-chr17 --runner=DataflowRunner" -Pdataflow-runner
   ``` 

* Note: Before running any of the below dataflow jobs, make sure that your files in the cloud bucket have the reqired access permissions (i.e., cloudservices.gserviceaccount.com, and compute@developer.gserviceaccount.com). Also, make sure the the Genomics API has been enabled in the Google API Manager Dashboard: https://console.developers.google.com/apis/api/genomics/.

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportAnnotationFromGCSToGG --datasetId=<Google_Genomics_DatasetId> --annotationSetName=sample_variant_annotation_chr17 --annotationReferenceSetId=EMWV_ZfLxrDY-wE --annotationInputTextBucketAddr=gs://<Your_Google_Cloud_Bucket_Name>/sample_variant_annotation_chr17.bed --runner=DataflowRunner --project=<Your_Google_Cloud_Project_Name> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --numWorkers=4 --type=VARIANT --header=chrom,chromStart,chromEnd,ref,alterBases,alleleFreq,dbsnpid --base0=no" -Pdataflow-runner
   ```

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportAnnotationFromGCSToGG --datasetId=<Google_Genomics_DatasetId> --annotationSetName=sample_transcript_annotation_chr17.bed --annotationReferenceSetId=EMWV_ZfLxrDY-wE --annotationInputTextBucketAddr=gs://<Your_Google_Cloud_Bucket_Name>/sample_transcript_annotation_chr17.bed --runner=DataflowRunner --project=<Your_Google_Cloud_Project_Name> --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --numWorkers=4 --type=Generic --base0=no --header=chrom,txStart,txEnd,bin,name,strand,cdsStart,cdsEnd,exonCount,exonStarts,exonEnds,score,name2,cdsStartStat,cdsEndStat,exonFrames" -Pdataflow-runner
   ```

 --->


