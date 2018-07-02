## Section 2: List Available Public Annotation Datasets
This part of the code demosntrates how to list AnnotationHive's public datasets.

* To list all available annotation datasets, run the following command:
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<YOUR_Google_Cloud_Project_ID> --bigQueryDatasetId=<Google_Cloud_Project_ID>:<BigQuery_Dataset_ID> --runner=DataflowRunner" -Pdataflow-runner   
   ```
   Note: bigQueryDatasetId for this command needs to have the full address including the project ID.  

* To list only annotation datasets with the same build, set `annotationDatasetBuild`. Here is an example for `hg19`: 

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<YOUR_Google_Cloud_Project_ID> --bigQueryDatasetId=<Google_Cloud_Project_ID>:<BigQuery_Dataset_ID> --annotationDatasetBuild=hg19 --runner=DataflowRunner" -Pdataflow-runner
   ```

* To find and list all annotation datasets which have a keyword, set `--searchKeyword`. Here is an example for `sample`:
    ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<YOUR_Google_Cloud_Project_ID> --bigQueryDatasetId=<Google_Cloud_Project_ID>:<BigQuery_Dataset_ID> --searchKeyword=sample --runner=DataflowRunner" -Pdataflow-runner
   ```

* To find and list all annotation datasets with the same type (i.e., generic or variant), set `--annotationType`. Here is an example for `generic`:
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<YOUR_Google_Cloud_Project_ID> --bigQueryDatasetId=<Google_Cloud_Project_ID>:<BigQuery_Dataset_ID> --annotationType=generic --runner=DataflowRunner" -Pdataflow-runner
   ```	

* You can also run any combonation of the abovementioned cases. For instance, all annotation datasets whih are `hg19`, and have a keyword `sample`, and they are `variant`:
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<YOUR_Google_Cloud_Project_ID> --bigQueryDatasetId=<Google_Cloud_Project_ID>:<BigQuery_Dataset_ID> --searchKeyword=Sample --annotationType=variant --annotationDatasetBuild=hg19 --runner=DataflowRunner" -Pdataflow-runner
   ``` 
 
