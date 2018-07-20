## Section 10: Import Private Annotation Datasets
This section explains how to import private annotation files to BigQuery.


### Upload ###
The first step in importing annotation files is to upload your local files to a bucket in Google.

* You can easily upload/remove/read/rename files in Google Cloud Storage using [gsutil] (https://cloud.google.com/storage/docs/gsutil) tool 

   ```
   gsutil cp Samples/sample_transcript_annotation_chr17.bed gs://<Your_Google_Cloud_Bucket_Name>/<DIR>
   ```

### Import COSMIC annotation files into BigQuery ###

To import annotation datasets into BigQuery, first run the following command to create an "AnnotationList" table for tracking annotation sets inside your BigQuery dataset.

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<Your_Google_cloud_Project> --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --createAnnotationSetListTable=true --runner=DataflowRunner " -Pdataflow-runner
   ```

### Import GETx annotation files into BigQuery ###
To import annotation datasets into BigQuery, first run the following command to create an "AnnotationList" table for tracking annotation sets inside your BigQuery dataset.

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<Your_Google_cloud_Project> --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --createAnnotationSetListTable=true --runner=DataflowRunner " -Pdataflow-runner
   ```

