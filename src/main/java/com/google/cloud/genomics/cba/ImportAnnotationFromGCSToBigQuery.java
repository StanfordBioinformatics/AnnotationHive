package com.google.cloud.genomics.cba;

/*
 * Copyright (C) 2016-2017 Stanford University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;



/**
 * <h1> Import Annotation Files (Reference Annotation Datasets)</h1> This class
 * imports annotations from a Google bucket to BigQuery. It prepares and converts
 * Annotation fields to the corresponding fields into a Google BigQuery table based on 
 * the provided header.
 * 
 * @version 1.0
 * @since 2016-07-01
 */

public class ImportAnnotationFromGCSToBigQuery {

	
	/*
	 * Options required to run this pipeline.
	 * 
	 * @param BigQueryDataset The ID of the Google BigQuery Dataset name that the output
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
	public static interface Options extends GenomicsOptions {

//		@Description("The ID of the Google Genomics Dataset that the output AnnotationSet will be posted to.")
//		@Default.String("")
//		String getDatasetId();
//		void setDatasetId(String datasetId);

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
		String getBigQueryDatasetId();
		void setBigQueryDatasetId(String BigQueryDatasetId);

		@Description("This provides BigQuery Table.")
		@Default.String("")
		String getBigQueryAnnotationSetTableId();
		void setBigQueryAnnotationSetTableId(String BigQueryAnnotationSetTableId);
	
		@Description("This provides whether the annotation is Varaiant or Transcript. This is a required field.")
		@Default.String("")
		String getAnnotationType();	
		void setAnnotationType(String VariantAnnotation);
		
		
		@Description("This provides the version annotationset. This is a required field.")
		@Default.String("")
		String getAnnotationSetVersion();	
		void setAnnotationSetVersion(String AnnotationSetVersion);
		
		
		@Description("This provides more info about the annotationset. This is an optional filed.")
		@Default.String("")
		String getAnnotationSetInfo();	
		void setAnnotationSetInfo(String AnnotationSetInfo);

		@Description("This provides assemblyId. This is a required field.")
		@Default.String("")
		String getAssemblyId();	
		void setAssemblyId(String AssemblyId);

		@Description("This provides the number of workers. This is a required filed.")
		int getNumWorkers();	
		void setNumWorkers(int value);

		@Description("This provides whether AnnotationSetList table exist or not. If not it will create the table.")
		@Default.Boolean(false)
		boolean getCreateAnnotationSetListTable();	
		void setCreateAnnotationSetListTable(boolean value);

	}

	private static Options options;
	private static Pipeline p;
	private static boolean DEBUG;
	private static final Logger LOG = Logger.getLogger(ImportAnnotationFromGCSToBigQuery.class.getName());

	public static void run(String[] args) throws GeneralSecurityException, IOException {


		////////////////////////////////// START TIMER /////////////////////////////////////  
		long startTime = System.currentTimeMillis();
		
		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		

		p = Pipeline.create(options);
		

		if (options.getBigQueryDatasetId().isEmpty()) {
			throw new IllegalArgumentException("BigQuery DatasetId must be specified");
		}

		if (options.getCreateAnnotationSetListTable()) {
			createAnnotationSetList();
			return;		
		}

		if (options.getBigQueryAnnotationSetTableId().isEmpty()) {
			throw new IllegalArgumentException("BigQuery AnnotationSet TableId must be specified");
		}
		
		if (options.getAnnotationInputTextBucketAddr().isEmpty()) {
			throw new IllegalArgumentException("annotationInputTextBucketAddr must be specified");
		}

		if (options.getAnnotationSetVersion().isEmpty()) {
			throw new IllegalArgumentException("AnnotationSet Version must be specified");
		}
		
		if (options.getHeader().isEmpty()) {
			throw new IllegalArgumentException("Header must be specified");
		}

		if (options.getBase0().isEmpty()) {
			throw new IllegalArgumentException("Base0 must be specified");
		}

		if (options.getAssemblyId().isEmpty()) {
			throw new IllegalArgumentException("AssemblyId must be specified");
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
		if (options.getAnnotationType().equalsIgnoreCase("variant")) {
			VariantAnnotation = true;
		} else if (options.getAnnotationType().equalsIgnoreCase("generic")) {
			VariantAnnotation = false;
		} else {
			throw new IllegalArgumentException("AnnotationType option must be either variant or generic!");
		}


		System.out.println(getTranscriptSchema().toString());
		
		String [] inputFields = options.getHeader().split(",");
		if (VariantAnnotation){
			System.out.println("Variant Annotation Pipeline");
			p.apply(TextIO.read().from(options.getAnnotationInputTextBucketAddr()))
			.apply(ParDo.of(new FormatFn(inputFields,VariantAnnotation,baseStatus)))
			.apply(
		            BigQueryIO.writeTableRows()
		                .to(getTable(options.getProject(), options.getBigQueryDatasetId(), options.getBigQueryAnnotationSetTableId()))
		                .withSchema(getVariantSchema())
		                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
		                .withWriteDisposition(WriteDisposition.WRITE_APPEND));
		}
		else{ //Generic
			System.out.println("Generic Annotation Pipeline");

			p.apply(TextIO.read().from(options.getAnnotationInputTextBucketAddr()))
			.apply(ParDo.of(new FormatFn(inputFields,VariantAnnotation,baseStatus)))
			.apply(
		            BigQueryIO.writeTableRows()
		                .to(getTable(options.getProject(), options.getBigQueryDatasetId(), options.getBigQueryAnnotationSetTableId()))
		                .withSchema(getTranscriptSchema())
		                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
		                .withWriteDisposition(WriteDisposition.WRITE_APPEND));			
		}

	    p.run().waitUntilFinish();

		////////////////////////////////// End TIMER /////////////////////////////////////  
		long tempEstimatedTime = System.currentTimeMillis() - startTime;
		addToAnnotationSetList(tempEstimatedTime);


			
	}
	
	
	
	
	  static TableReference getTable(String projectId, String datasetId, String tableName) {
		    TableReference table = new TableReference();
		    table.setDatasetId(datasetId);
		    table.setProjectId(projectId);
		    table.setTableId(tableName);
		    return table;
		  }
	
	
	/**
	 * <h1>This function add a new table called AnnotationList.
	 * AnnotationList is a table containing 
	 * the information about annotation sets. 
	 *
	 */	  
	public static void createAnnotationSetList() {
		
	    BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();
	    BigQuery bigquery = optionsBuilder.build().getService();

	    Table existTable = bigquery.getTable(options.getBigQueryDatasetId(), "AnnotationList");

	    if(existTable==null){	    	
	    	List<Field> fields = new ArrayList<Field>();
	    	fields.add(Field.of("AnnotationSetName", Field.Type.string()));
	    	fields.add(Field.of("AnnotationSetVersion", Field.Type.string()));
	    	fields.add(Field.of("AnnotationSetType", Field.Type.string()));
	    	fields.add(Field.of("AnnotationSetFields", Field.Type.string()));
	    	fields.add(Field.of("CreationDate", Field.Type.string()));
	    	fields.add(Field.of("HumanAssembly", Field.Type.string()));
	    	fields.add(Field.of("AnnotationSetSize", Field.Type.string()));
	    	fields.add(Field.of("info", Field.Type.string()));
			Schema schema = Schema.of(fields);		
			TableId tableId = TableId.of(options.getBigQueryDatasetId(), "AnnotationList");

			TableDefinition tableDefinition = StandardTableDefinition.of(schema);
			TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
			bigquery.create(tableInfo);
		    System.out.println("### Successfully created "+options.getProject() +":"+  options.getBigQueryDatasetId() + ".AnnotationList table.");

		    Map<String, Object> rowContent = new HashMap<>();
		    rowContent.put("AnnotationSetName", "Test");
		    rowContent.put("AnnotationSetVersion", "Test");
		    rowContent.put("AnnotationSetType", "Test");
		    rowContent.put("AnnotationSetFields", "Test");
		    rowContent.put("CreationDate", "Test");
		    rowContent.put("HumanAssembly", "Test");
		    rowContent.put("AnnotationSetSize", "Test");
		    rowContent.put("Info", "Test");
		    InsertAllResponse response;
	    		response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId).addRow("rowId", rowContent).build());
		    if(DEBUG){
		    		System.out.println(response.toString());
		    		LOG.warning(response.toString());
		    }
	    }
	    else
		    System.out.println("### Table \"AnnotationList\" exists");

	}

	
	/**
	 * <h1>This function adds information about the annotation set 
	 * to the AnnotationList table. AnnotationList is a table containing 
	 * the information about annotation sets 
	 *
	 */	
	static void addToAnnotationSetList(long tempEstimatedTime) {
	
	    BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();
	    BigQuery bigquery = optionsBuilder.build().getService();

	    
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
	    TableId tableId = TableId.of(options.getBigQueryDatasetId(), "AnnotationList");
	    Map<String, Object> rowContent = new HashMap<>();
	    rowContent.put("AnnotationSetName", options.getBigQueryAnnotationSetTableId());
	    rowContent.put("AnnotationSetVersion", options.getAnnotationSetVersion());
	    rowContent.put("AnnotationSetType", options.getAnnotationType());
	    rowContent.put("AnnotationSetFields", options.getHeader());
	    rowContent.put("CreationDate", dateFormat.format(date).toString());
	    rowContent.put("HumanAssembly", options.getAssemblyId());

		QueryResponse rs = BigQueryFunctions.runquery(BigQueryFunctions.countQuery(options.getProject(), options.getBigQueryDatasetId(), options.getBigQueryAnnotationSetTableId()));
		QueryResult result = rs.getResult();
		String numAnnotations="0";
		if (result != null)
			for (List<FieldValue> row : result.iterateAll()) {
				numAnnotations=row.get(0).getValue().toString();
		}
		
		System.out.println("### number of annotations added: " + numAnnotations);
		rowContent.put("AnnotationSetSize", numAnnotations);
		if(options.getNumWorkers()!=0)
			rowContent.put("Info", options.getAnnotationSetInfo() + "\t Number of Instances: " + options.getNumWorkers() + " ExecutionTime (msec): " + tempEstimatedTime);
		else
			rowContent.put("Info", options.getAnnotationSetInfo() + "\t Number of Instances: AutoScaling ExecutionTime (msec): " + tempEstimatedTime);

	    
	    InsertAllResponse response;
	    response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId).addRow(rowContent).build());
	    
	    System.out.println(response.toString());
	    if(!response.getInsertErrors().isEmpty())
	    		System.out.println("### Error in inserting a new table to AnnotationSetList Table: " + response.getInsertErrors().toString());
	    else
	    		System.out.println("### Successfully added a new annotationSet table: " + rowContent.toString());
	}

	/**
	 * <h1>This function defines the BigQuery table schema 
	 * used for variant annotation tables.
	 *  (e.g., 'chrm', 'start', 'end', 'base', 'alt', ...)
	 * @return schema 
	 */
	
	static TableSchema getVariantSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("chrm").setType("STRING"));
		fields.add(new TableFieldSchema().setName("start").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("end").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("base").setType("STRING"));
		fields.add(new TableFieldSchema().setName("alt").setType("STRING"));
		
		String[] inputFields= options.getHeader().split(",");
		for(int i=5; i<inputFields.length; i++){
			fields.add(new TableFieldSchema().setName(inputFields[i]).setType("STRING"));
		}

		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}
	
	/**
	 * <h1>This function defines the BigQuery table schema 
	 * used for generic annotation tables.
	 *  (e.g., 'chrm', 'start', 'end', ...)
	 * @return schema 
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
	 * <h1>This function remove "chr" from the beginning of referenceName
	 * (e.g, chr17 becomes 17)
	 * 
	 * @param refName
	 *            Reference Name
	 * @return refName Reference Name w/o "chr"
	 */
	private static String canonicalizeRefName(String refName) {
		return refName.replace("chr", "");
	}	

	/**
	 * <h1>This function formats the results to a TableRow, to save in BigQuery Table.
	 * 
	 * @param inputFields All fields of tables
	 * @param VariantAnnotation Type of imported annotation table (e.g., variant or generic)
	 * @param is_0_Base Define whether the annotation set is 1-base (False) or 0-base (True)  
	 * @return row formatted TableRow
	 */

	static class FormatFn extends DoFn<String, TableRow> {

		private final String [] inputFields;
		private final boolean VariantAnnotation;
		private final boolean is_0_Base;
		private static final long serialVersionUID = 7700800981719306804L;

		public FormatFn(String [] inputFields, boolean VA, boolean base0) {
			this.inputFields = inputFields;
			this.VariantAnnotation = VA;
			this.is_0_Base = base0;
			}

		@org.apache.beam.sdk.transforms.DoFn.ProcessElement
		public void processElement(ProcessContext c) {
			for (String line : c.element().split("\n")) {
				if (!line.isEmpty() && !line.startsWith("#") && !line.startsWith("chrom")) {

					String[] vals = c.element().toString().split("\t");
		
					
					TableRow row = new TableRow();
					
					//Variant
					if (this.VariantAnnotation){
						
						row.set("chrm", canonicalizeRefName(vals[0]));
						
						if (!this.is_0_Base){
								row.set("start", Integer.parseInt(vals[1])-1);
						}
						else{
							row.set("start", Integer.parseInt(vals[1]));
						}
						
						row.set("end", vals[2])
						.set("base", vals[3])
						.set("alt", vals[4]);
					
						for (int index=5; index<vals.length; index++ )
							row.set(inputFields[index], vals[index]);
					

					}
					else{ //Generic+Transcript
						
						row.set("chrm", canonicalizeRefName(vals[0]));
						
						if (!this.is_0_Base){
							row.set("start", Integer.parseInt(vals[1])-1);
						}
						else{
							row.set("start", Integer.parseInt(vals[1]));
						}
						
						row.set("end", vals[2]);				

						for (int index=3; index<vals.length; index++ )
							row.set(inputFields[index], vals[index]);
								
						
					}
					
					c.output(row);
				}
			}
		}
	}

}

