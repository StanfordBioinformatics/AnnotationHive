## Section 3: Variant-based Annotation
This section explains how to annotate a VCF/mVCF table against any number of variant-based annotation datasets. 


## mVCF/VCF ##

After importing mVCF/VCF files successfully (refer to [Section 1](Import.md)), then to run them against a number of
annotation datasets (to list annotation datasets refer to [Section 2](List-Annotation.md)).

Here are the key parameters:

* **--bigQueryDatasetId**:
* **--localOutputFilePath**: specify this file when you want to sort the output of BigQuery using BigQuery itself
* **--VCFTables**: The BigQuery address of the mVCF/VCF table on BigQuery
* **--VCFCanonicalizeRefNames**: This provides the prefix for reference field in VCF tables (e.g, "chr"). AnnotationHive automatically canonicalizes the VCF table by removing the prefix in its calculation. 
* **--projectId**: The project ID that has access to the VCF tables.
* **--bucketAddrAnnotatedVCF**: This provides the full bucket and name address to the output VCF File (e.g., gs://mybucket/outputVCF.vcf).
* **--variantAnnotationTables**: This provides the address of variant annotation tables (e.g., gbsc-gcp-project-cba:AnnotationHive.hg19_UCSC_snp144).
* **--createVCF**: If you want to get a VCF file, then set this flag true (default value is false, and it creates a table).

### AnnotationHive VCF Table ###
If you imported mVCF or VCF file using AnnotationHive's API, then you can 


### Google VCF Table ###

 
