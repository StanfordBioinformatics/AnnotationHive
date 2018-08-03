## Section 10: Import Private Annotation Datasets
This section explains how to import private annotation files to BigQuery.


### Upload ###
The first step in importing annotation files is to upload your local files to a bucket in Google.

* You can easily upload/remove/read/rename files in Google Cloud Storage using [gsutil] (https://cloud.google.com/storage/docs/gsutil) tool.

Upload one sample of COSMIC vcf file. Use `prepare_annovar_user.pl` script from Annovar to prepare the file to be imported (http://annovar.openbioinformatics.org/en/latest/user-guide/filter/#cosmic-annotations). 
   ```
   gsutil cp Samples/COSMIC_coding_edited_sample.txt gs://<Your_Google_Cloud_Bucket_Name>/<DIR>
   ```

Upload one sample of GTEx tissue-specific file: 

   ```
   gsutil cp Samples/GTEx_Adipose_Subcutaneous.txt gs://<Your_Google_Cloud_Bucket_Name>/<DIR>
   ```

### Import COSMIC annotation files into BigQuery ###
Download the VCF files from [COSMIC website] (https://cancer.sanger.ac.uk/cosmic/download). Note that you will need to login to download the files. It is free for academic oragnizations.

To import annotation datasets into BigQuery, first run the following command to create an "AnnotationList" table for tracking annotation sets inside your BigQuery dataset.

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<Your_Google_cloud_Project> --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --createAnnotationSetListTable=true --runner=DataflowRunner " -Pdataflow-runner
   ```
Then, run the following command:
   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportAnnotationFromGCSToBigQuery --project=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --annotationInputTextBucketAddr=gs://<Your_Google_Cloud_Bucket_Name>/COSMIC_coding_edited_sample.txt --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --annotationType=variant --header=CHROM,START,END,REF,ALT,INFO --base0=no --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --bigQueryAnnotationSetTableId=COSMIC_coding_edited_sample --annotationSetVersion=1.0 --assemblyId=GRCh38 --columnOrder=1,2,3,4,5 --annotationSetInfo='Source: AnnotationHive/Samples'" -Pdataflow-runner

   ```

### Import GETx annotation files into BigQuery ###
Download Tissue-Specific SNP files from [GTEx website] (https://www.gtexportal.org/home/datasets). Note that you will need to sign in with your Google ID.

To import annotation datasets into BigQuery, first run the following command to create an "AnnotationList" table for tracking annotation sets inside your BigQuery dataset.

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="BigQueryAnnotationRepository --project=<Your_Google_cloud_Project> --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --createAnnotationSetListTable=true --runner=DataflowRunner " -Pdataflow-runner
   ```
Then, run the following command:

   ```
   mvn compile exec:java -Dexec.mainClass=com.google.cloud.genomics.cba.StartAnnotationHiveEngine -Dexec.args="ImportAnnotationFromGCSToBigQuery --project=<Your_Google_Cloud_Project_Name> --runner=DataflowRunner --annotationInputTextBucketAddr=gs://<Your_Google_Cloud_Bucket_Name>/GTEx_Adipose_Subcutaneous.txt --stagingLocation=gs://<Your_Google_Cloud_Bucket_Name>/<Staging_Address>/ --annotationType=variant --header=gene_id,gene_name,gene_chr,gene_start,gene_end,strand,num_var,beta_shape1,beta_shape2,true_df,pval_true_df,variant_id,tss_distance,chr,pos,ref,alt,num_alt_per_site,rs_id_dbSNP147_GRCh37p13,minor_allele_samples,minor_allele_count,maf,ref_factor,pval_nominal,slope,slope_se,pval_perm,pval_beta,qval,pval_nominal_threshold,log2_aFC,log2_aFC_lower,log2_aFC_upper --base0=no --bigQueryDatasetId=<YOUR_BigQuery_Dataset_ID> --bigQueryAnnotationSetTableId=GTEx_Adipose_Subcutaneous --annotationSetVersion=1.0 --assemblyId=GRCh37 --columnOrder=14,15,15,16,17 --POS=true --annotationSetInfo='Source: AnnotationHive/Samples'" -Pdataflow-runner

   ```
