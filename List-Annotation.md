## Section 2: List Available Public Annotation Datasets
This part of the code demosntrates how to list AnnotationHive's public datasets.

* ****: To list all available annotation datasets, run the following command:
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<YOUR_Google_Cloud_Project_ID> --bigQueryDatasetId=<Google_Cloud_Project_ID>:<BigQuery_Dataset_ID> --runner=DataflowRunner" -Pdataflow-runner   
   ```
   Note: bigQueryDatasetId for this command needs to have the full address including the project ID.  

* ****: 


