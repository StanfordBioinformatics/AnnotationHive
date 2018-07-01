## Section 7: Sample Experiments
This section provides sevarl experiments on scalability and the cost of the system.

### AnnotationHive vs. Annovar ###

In the following experiment, we tested AnnotationHive and 
Annovar for one sample (HG00096) of 1000 genomes with over 4.2M 
variants, and for all 1000 samples with over 85.2M variants against 
the following five annotation datasets:

![Annotation Datasets](https://github.com/StanfordBioinformatics/cloud-based-annotation/blob/master/common/img/Annotations.png "Annotation Datasets")

---

### Execution Time ###

Over 16B annotation records were processed. The y-axis is logarithmic and represents the execution time in minutes. 
The number of variants is depicted on the x-axis. In both cases, AnnotationHive is around two orders of magnitude faster 
than Annovar. For this experiment, we used n1-highmem-16 instances for Annovar and AnnotationHive's Dataflow sort function. 

![AnnotationHive vs. Annovar](https://github.com/StanfordBioinformatics/cloud-based-annotation/blob/master/common/img/Experiment_AnnotationHive_BigQuery.png "AnnotationHive vs. Annovar")

---

### Accuracy ###

We compared the annotated VCF files for the BRCA1 region. All records are the same except three records with 
genotype values of 0 where Annovar considered them in the output. We filter out variants with every genotype
value less than or equal 0.
 


