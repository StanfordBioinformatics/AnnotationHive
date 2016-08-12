package com.google.cloud.genomics.cba;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.RetryPolicy;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.ImportVariantsRequest;
import com.google.api.services.genomics.model.Operation;
import com.google.api.services.genomics.model.VariantSet;
import com.google.api.services.genomics.Genomics.Variantsets;

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

public class ImportVCF {

	private static Options options;
	private static OfflineAuth auth;

	public static interface Options extends ShardOptions {

		@Description("The ID of the Google Genomics Dataset")
		@Default.String("")
		String getDatasetId();
		void setDatasetId(String datasetId);

		@Description("This provides the input of VCF source URIs. This is a required field.")
		@Default.String("")
		String getURIs();
		void setURIs(String uri);

		@Description("This provides the name of variantSet. This is a required field.")
		@Default.String("")
		String getVariantSetName();
		void setVariantSetName(String name);
	}

	/**
	 * <h1> This method is the main point of entry in this class; it creates a
	 * variantSet, and import VCF files
	 */

	public static void run(String[] args) throws GeneralSecurityException, IOException {

		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		auth = GenomicsOptions.Methods.getGenomicsAuth(options);

		if (options.getDatasetId().isEmpty()) {
			throw new IllegalArgumentException("datasetId must be specified");
		}

		if (options.getURIs().isEmpty()) {
			throw new IllegalArgumentException("URIs of the input VCF files must be specified (e.g., "
					+ "gs://bucket/variants.vcf,gs://bucket/variants2.vcf,gs://bucket/variants_chr*.vcf)");
		}

		if (options.getVariantSetName().isEmpty()) {
			throw new IllegalArgumentException("--variantSetName = Name of the variantSet must be specified");
		}
		try{
			VariantSet variantSet = createVariantSet();
	
			ImportVariantsRequest IVReq = new ImportVariantsRequest();
			List<String> URIs = new ArrayList<String>(Arrays.asList(options.getURIs().split(",")));
	
			IVReq.setSourceUris(URIs);
			IVReq.setVariantSetId(variantSet.getId());
	
			Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);
			RetryPolicy retryP = RetryPolicy.nAttempts(4);
	
			// TODO: Wait till the job is completed (Track the job)
			Operation response = retryP.execute(genomics.variants().genomicsImport(IVReq));
			
			
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

	/**
	 * <h1> This method creates a new variant set using the given datasetId.
	 */

	private static VariantSet createVariantSet() throws GeneralSecurityException, IOException {

		VariantSet vs = new VariantSet();
		vs.setName(options.getVariantSetName());
		vs.setDatasetId(options.getDatasetId());

		Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);
		Variantsets.Create avRequest = genomics.variantsets().create(vs);
		VariantSet avWithId = avRequest.execute();
		System.out.println(avWithId.toPrettyString());
		return avWithId;
	}

}
