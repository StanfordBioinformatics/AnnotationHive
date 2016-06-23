# README #

### Cloud-based Annotation Engine ###

* Annotation is the process by which pertinent information about raw DNA sequences is added to genome databases. Multiple software applications have been developed to annotate genetic variants that can be automatically derived  from diverse genomes (e.g., ANNOVAR, SnpEff). The first problem using the existing tools is about downloading the software and the large build files. The second problem is scalability. Because current tools are sequential, the annotation of large numbers of patients is tedious and takes a significant amount of time. 

* The pay-as-you-go model of cloud computing, which removes the maintenance effort required for a high performance computing (HPC) facility while simultaneously offering elastic scalability, is well suited for genomic analysis.

* In this project, we developed a cloud-based annotation engine that automatically annotate the user’s input datasets (e.g., VCF, avinput files) in the cloud.

* Version 1.0

### How do I get set up? ###

* Install Apache Maven 3.3.3 
* cd cloud-based-annotation/java
* mvn javadoc:javadoc
* mvn assembly:assembly
* java -jar target/cba-v1-jar-with-dependencies.jar <ReferenceSetId> <DatasetId> <API_KEY> <Input_bucket_name and input_filename> <Input_bucket_name and output_filename>
