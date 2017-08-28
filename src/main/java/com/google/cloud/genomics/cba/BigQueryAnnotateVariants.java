package com.google.cloud.genomics.cba;

import java.util.logging.Logger;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.genomics.cba.ImportAnnotation.Options;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.OfflineAuth;

public class BigQueryAnnotateVariants {
	
	public static interface Options extends ShardOptions {

		@Validation.Required
		@Description("The ID of the Google Genomics Dataset that the output AnnotationSet will be posted to.")
		@Default.String("")
		String getDatasetId();

		void setDatasetId(String datasetId);

		@Description("This provides the name for the AnnotationSet. Default (empty) will set the "
				+ "name to the input References. For more information on AnnotationSets, please visit: "
				+ "https://cloud.google.com/genomics/v1beta2/reference/annotationSets#resource")
		@Default.String("")
		String getAnnotationSetName();

		void setAnnotationSetName(String name);

		@Description("This provides the refernceSetId for the AnnotationSet. This is a required field.")
		@Default.String("")
		String getAnnotationReferenceSetId();

		void setAnnotationReferenceSetId(String referenceSetId);

		@Description("This provides the URI of output Json file. This file contains Json version of annotations. This is a required field.")
		@Default.String("")
		String getAnnotationOutputJSONBucketAddr();

		void setAnnotationOutputJSONBucketAddr(String annotationOutputJSONBucketAddr);

		@Description("This provides the URI of inputfile which contains annotations. This is a required field.")
		@Default.String("")
		String getAnnotationInputTextBucketAddr();

		void setAnnotationInputTextBucketAddr(String annotationInputTextBucketAddr);

		@Description("This provides the type for the AnnotationSet. This is a required field.")
		@Default.String("")
		String getType();

		void setType(String type);

		@Description("This provides whether the base is 0 or 1. This is a required field.")
		@Default.String("")
		String getBase0();

		void setBase0(String base0);

		@Description("This provides the header for the Annotations. This is a required field. "
				+ "(It must start w/ the following fields: \"Chrom,start,end,ref,alter,all other fileds\")")
		@Default.String("")
		String getHeader();

		void setHeader(String header);

		@Description("This provides BigQuery Dataset ID.")
		@Default.String("")
		String getBigQueryDataset();

		void setBigQueryDataset(String BigQueryDataset);

		@Description("This provides BigQuery Table.")
		@Default.String("")
		String getBigQueryTable();

		void setBigQueryTable(String BigQueryTable);

		
		@Description("This provides the path to the local output file.")
		@Default.String("")
		String getLocalOutputFilePath();

		void setLocalOutputFilePath(String LocalOutputFilePath);
	}

	private static Options options;
	private static Pipeline p;
	private static OfflineAuth auth;
	private static final Logger LOG = Logger.getLogger(ImportAnnotation.class.getName());

	/**
	 * <h1>This function is the main function that creates and calls dataflow
	 * pipeline
	 */
	public static void run(String[] args) throws Exception {
	
	String	VCFTableName="genomics-public-data:platinum_genomes.variants", 
			TranscriptTableNames="", 
			VariantTableNames="gbsc-gcp-project-cba:annotation.Cosmic68"
					+ ",gbsc-gcp-project-cba:annotation.Cosmic68_2"
					+ ",gbsc-gcp-project-cba:annotation.Cosmic68_3"
					+ ",gbsc-gcp-project-cba:annotation.Cosmic68_4"
					+ ",gbsc-gcp-project-cba:annotation.Cosmic68_5";

	String queryString = BigQueryFunctions.prepareAnnotateVariantQuery(VCFTableName, TranscriptTableNames, VariantTableNames);
	
	System.out.println("Query: " + queryString);

	//////////////////////////////////STEP1: Run Joins/////////////////////////////////////  
	long startTime = System.currentTimeMillis();
	
	BigQueryFunctions.runQueryPermanentTable(queryString, options.getBigQueryDataset(), 
			options.getBigQueryTable(), true);
	long tempEstimatedTime = System.currentTimeMillis() - startTime;
	System.out.println("Execution Time for Join Query: " + tempEstimatedTime);
	//////////////////////////////////////////////////////////////////////////////////////	
	
	
	//////////////////////////////////STEP2: Run Sort/////////////////////////////////////  
	startTime = System.currentTimeMillis();

	BigQueryFunctions.sort(options.getProject(), options.getBigQueryDataset(), 
			options.getBigQueryTable(), options.getLocalOutputFilePath());
	
	tempEstimatedTime = System.currentTimeMillis() - startTime;
	System.out.println("Execution Time for Sort Query: " + tempEstimatedTime);
	//////////////////////////////////////////////////////////////////////////////////////
	
	//////////////////////////////STEP2: Delete the Intermediate Table ///////////////////  
	BigQueryFunctions.deleteTable(options.getBigQueryDataset(), options.getBigQueryTable());
	
	}
}
