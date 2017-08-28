package com.google.cloud.genomics.cba;

import java.io.IOException;
import java.security.GeneralSecurityException;


import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.RetryPolicy;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.ExportVariantSetRequest;
import com.google.api.services.genomics.model.Operation;


/**
 * <h1>Import VCF Files</h1> This class creates a variantSet imports VCF files.
 * 
 * @param datasetId
 *            The dataset to which this annotation set belongs.
 * @param name
 *            The name of the variantSet.
 * @param URIs
 *            The comma-delimited list of URIs.
 * 
 * @version 1.0
 * @since 2016-07-01
 */

public class ExportVCF {

	private static Options options;
	private static OfflineAuth auth;

	public static interface Options extends ShardOptions {

		@Description("The ID of the Google Genomics Dataset")
		@Default.String("")
		String getGoogleGenomicsDatasetId();
		void setGoogleGenomicsDatasetId(String GoogleGenomicsDatasetId);

		@Description("The ID of the Google Genomics Dataset")
		@Default.String("")
		String getProjectId();
		void setProjectId(String ProjectId);
		
		@Description("This provides the name of the destination BigQuery Table. This is a required field.")
		@Default.String("")
		String getBigQueryTableId();
		void setBigQueryTableId(String BigQueryTableId);

		@Description("This provides variantSetId. This is a required field.")
		@Default.String("")
		String getVariantSetId();
		void setVariantSetId(String VariantSetId);
		
		@Description("This provides BigqueryDataSetId. This is a required field.")
		@Default.String("")
		String getBigQueryDataSetId();
		void setBigQueryDataSetId(String BigqueryDataSetId);
	}

	/**
	 * <h1> This method is the main point of entry in this class; it creates a table in BigQuery
	 * and export variantSet to the BigQuery Table
	 */
	
	 public static void run(String[] args) throws GeneralSecurityException, IOException {

		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		auth = GenomicsOptions.Methods.getGenomicsAuth(options);

//		if (options.getGoogleGenomicsDatasetId().isEmpty()) {
//			throw new IllegalArgumentException("googleGenomicsDatasetId must be specified");
//		}

		if (options.getProjectId().isEmpty()) {
			throw new IllegalArgumentException("projectId must be specified");
		}
		
		if (options.getVariantSetId().isEmpty()) {
			throw new IllegalArgumentException("variantSetId must be specified");
		}

		if (options.getBigQueryDataSetId().isEmpty()) {
			throw new IllegalArgumentException("bigqueryDataSetId must be specified");
		}
		
		if (options.getBigQueryTableId().isEmpty()) {
			throw new IllegalArgumentException("bigqueryTableId must be specified");
		}
		try{
	
		
			Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);
			RetryPolicy retryP = RetryPolicy.nAttempts(4);
	
		    ExportVariantSetRequest requestBody = new ExportVariantSetRequest();
		    requestBody.setBigqueryTable(options.getBigQueryTableId());
		    requestBody.setBigqueryDataset(options.getBigQueryDataSetId());
		    requestBody.setProjectId(options.getProject());

		    
			// TODO: Wait till the job is completed (Track the job)
		    Operation response = retryP.execute(genomics.variantsets().export(options.getVariantSetId(), requestBody));
			
			
			System.out.println("");
			System.out.println("");
			System.out.println("[INFO] ------------------------------------------------------------------------");
			System.out.println("[INFO] Opertaion INFO:");
			System.out.println(response.toPrettyString());
			System.out.println("[INFO] To check the current status of your job, use the following command:");
			System.out.println("\t ~: gcloud alpha genomics operations describe $operation-id$");
			System.out.println("[INFO] ------------------------------------------------------------------------");
			System.out.println("");
			System.out.println("");
		}
		catch(Exception e){
			throw e;
		}

	}


}
