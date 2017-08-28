package com.google.cloud.genomics.cba;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.Genomics.Annotationsets;
import com.google.api.services.genomics.model.Annotation;
import com.google.api.services.genomics.model.AnnotationSet;
import com.google.api.services.genomics.model.BatchCreateAnnotationsRequest;
import com.google.api.services.genomics.model.BatchCreateAnnotationsResponse;
import com.google.api.services.genomics.model.Transcript;
import com.google.api.services.genomics.model.VariantAnnotation;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.cba.AnnotateVariants.ConvertBigQueryFormat;
import com.google.cloud.genomics.cba.AnnotateVariants.FormatStatsFn;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.RetryPolicy;
import com.google.common.collect.Lists;
import com.google.genomics.v1.StreamVariantsRequest;

/**
 * <h1> Import Annotation Files (Reference Annotation Datasets)</h1> This class
 * imports annotations from a Google bucket to BigQuery. It prepares and converts
 * Annotation fields to the corresponding fields into a Google BigQuery table based on 
 * the provided header.
 * 
 * @version 1.0
 * @since 2016-07-01
 */

public class BigQueryImportAnnotation {

	
	/*
	 * Options required to run this pipeline.
	 * 
	 * @param datasetId The ID of the Google Genomics Dataset that the output
	 * AnnotationSet will be posted.
	 * 
	 * @param annotationInputTextBucketAddr This provides the URI of inputfile
	 * which contains annotations.
	 * 
	 * @param type This provides the type of reference annotation dataset
	 * 
	 * @param base0 This provides whether the reference annotations are 0-Based
	 * or 1-Based.
	 */
	public static interface Options extends ShardOptions {

		@Validation.Required
		@Description("The ID of the Google Genomics Dataset that the output AnnotationSet will be posted to.")
		@Default.String("")
		String getDatasetId();

		void setDatasetId(String datasetId);


		@Description("This provides the URI of inputfile which contains annotations. This is a required field.")
		@Default.String("")
		String getAnnotationInputTextBucketAddr();

		void setAnnotationInputTextBucketAddr(String annotationInputTextBucketAddr);

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

		
		@Description("This provides whether the annotation is Varaiant or Transcript. This is a required field.")
		@Default.String("")
		String getVariantAnnotation();
		
		void setVariantAnnotation(String VariantAnnotation);
		
	}

	private static Options options;
	private static Pipeline p;
	private static OfflineAuth auth;
	private static final Logger LOG = Logger.getLogger(ImportAnnotation.class.getName());

	public static void run(String[] args) throws GeneralSecurityException, IOException {
		// Register the options so that they show up via --help
		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		auth = GenomicsOptions.Methods.getGenomicsAuth(options);

		p = Pipeline.create(options);
		p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

		if (options.getDatasetId().isEmpty()) {
			throw new IllegalArgumentException("datasetId must be specified");
		}

		if (options.getAnnotationInputTextBucketAddr().isEmpty()) {
			throw new IllegalArgumentException("annotationInputTextBucketAddr must be specified");
		}


		if (options.getHeader().isEmpty()) {
			throw new IllegalArgumentException("Header must be specified");
		}

		if (options.getBase0().isEmpty()) {
			throw new IllegalArgumentException("Base0 must be specified");
		}

		boolean baseStatus = false;
		if (options.getBase0().equalsIgnoreCase("YES")) {
			baseStatus = true;
		} else if (options.getBase0().equalsIgnoreCase("NO")) {
			baseStatus = false;
		} else {
			throw new IllegalArgumentException("Base0 option must be either yes or no!");
		}

		boolean VariantAnnotation = false;
		if (options.getVariantAnnotation().equalsIgnoreCase("YES")) {
			VariantAnnotation = true;
		} else if (options.getVariantAnnotation().equalsIgnoreCase("NO")) {
			VariantAnnotation = false;
		} else {
			throw new IllegalArgumentException("Variant Annotation option must be either yes or no!");
		}


		/////////////////////////////////////////////////////////////
		TableReference tableRef = new TableReference();
		tableRef.setProjectId(options.getProject());
		tableRef.setDatasetId(options.getBigQueryDataset());
		tableRef.setTableId(options.getBigQueryTable());

		System.out.println(getTranscriptSchema().toString());
		
		String [] inputFields = options.getHeader().split(",");
		if (VariantAnnotation){
			System.out.println("Variant Annotation Pipeline");
			p.apply(TextIO.Read.from(options.getAnnotationInputTextBucketAddr()))
					.apply(new ConvertBigQueryVariantFormat(inputFields,VariantAnnotation,baseStatus))
					.apply(BigQueryIO.Write.to(tableRef).withSchema(getVariantSchema()));
		}
		else{ //Generic
			System.out.println("Generic Annotation Pipeline");

			p.apply(TextIO.Read.from(options.getAnnotationInputTextBucketAddr()))
			.apply(new ConvertBigQueryVariantFormat(inputFields,VariantAnnotation,baseStatus))
			.apply(BigQueryIO.Write.to(tableRef).withSchema(getTranscriptSchema()));
		}

		p.run();
		

	}


	/**
	 * Defines the BigQuery schema used for the output.
	 */
	static TableSchema getVariantSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("chrm").setType("STRING"));
		fields.add(new TableFieldSchema().setName("start").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("end").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("bases").setType("STRING"));
		fields.add(new TableFieldSchema().setName("alt").setType("STRING"));
		
		String[] inputFields= options.getHeader().split(",");
		for(int i=5; i<inputFields.length; i++){
			fields.add(new TableFieldSchema().setName(inputFields[i]).setType("STRING"));
		}

		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}

	/**
	 * Defines the BigQuery schema used for the output.
	 */
	static TableSchema getTranscriptSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("chrm").setType("STRING"));
		fields.add(new TableFieldSchema().setName("start").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("end").setType("INTEGER"));
		
		String[] inputFields= options.getHeader().split(",");
		
		for(int i=3; i<inputFields.length; i++){
			fields.add(new TableFieldSchema().setName(inputFields[i]).setType("STRING"));
		}
		
		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}

	/**
	 * This PTransform extracts speed info from traffic station readings. 
	 * It formats the results for BigQuery.
	 */
	static class ConvertBigQueryVariantFormat extends PTransform<PCollection<String>, PCollection<TableRow>> {
		
		
		/**
		* 
		*/
		
		private final String [] inputFields;
		private final boolean VariantAnnotation;
		private final boolean is_0_Base;

		private static final long serialVersionUID = 1L;
	
		public ConvertBigQueryVariantFormat(String [] inputFields, boolean VA, boolean base0) {
			this.inputFields = inputFields;
			this.VariantAnnotation = VA;
			this.is_0_Base = base0;
		}
		
		@Override
		public PCollection<TableRow> apply(PCollection<String> annotatedVariants) {

			// Format the results for writing to BigQuery
			PCollection<TableRow> results = annotatedVariants.apply(ParDo.of(new FormatFn(inputFields,VariantAnnotation,is_0_Base )));

			return results;
		}
	}
	
	

	/**
	 * Format the results to a TableRow, to save to BigQuery.
	 */
	static class FormatFn extends DoFn<String, TableRow> {
		/**
		 * 
		 */
		private final String [] inputFields;
		private final boolean VariantAnnotation;
		private final boolean is_0_Base;
		private static final long serialVersionUID = 7700800981719306804L;

		public FormatFn(String [] inputFields, boolean VA, boolean base0) {
			this.inputFields = inputFields;
			this.VariantAnnotation = VA;
			this.is_0_Base = base0;
			}

		@Override
		public void processElement(ProcessContext c) {
			for (String line : c.element().split("\n")) {
				if (!line.isEmpty() && !line.startsWith("#") && !line.startsWith("chrom")) {

					String[] key = c.element().toString().split("\t");
		
					
					TableRow row = new TableRow();
					
					//Variant
					if (this.VariantAnnotation){
						
						row.set("chrm", key[0]);
						
						if (!this.is_0_Base){
								row.set("start", Integer.parseInt(key[1])-1);
						}
						else{
							row.set("start", Integer.parseInt(key[1]));
						}
						
						row.set("end", key[2])
						.set("bases", key[3])
						.set("alt", key[4]);
					
						for (int index=5; index<key.length; index++ )
							row.set(inputFields[index], key[index]);
						
					}
					else{ //Generic+Transcript
						
						row.set("chrm", key[0]);
						
						if (!this.is_0_Base){
							row.set("start", Integer.parseInt(key[1])-1);
						}
						else{
							row.set("start", Integer.parseInt(key[1]));
						}
						
						row.set("end", key[2]);				

						for (int index=3; index<key.length; index++ )
							row.set(inputFields[index], key[index]);
										
					}
					
					c.output(row);
				}
			}
		}
	}

}

