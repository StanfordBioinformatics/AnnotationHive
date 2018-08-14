package com.google.cloud.genomics.cba;

/*
 * Copyright (C) 2016-2018 Stanford University.
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
import org.apache.beam.sdk.PipelineResult.State;
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
	
		@Description("This provides whether the annotation is Varaiant or Generic. This is a required field.")
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

		@Description("This provides columnOrder for the first 3 columns for a generic annotations and 5 for "
				+ " a variant annotation. This is an optional field.")
		@Default.String("")
		String getColumnOrder();	
		void setColumnOrder(String ColumnOrder);

		@Description("This provides the number of workers. This is a required filed.")
		int getNumWorkers();	
		void setNumWorkers(int value);

		@Description("This provides whether AnnotationSetList table exist or not. If not it will create the table.")
		@Default.Boolean(false)
		boolean getCreateAnnotationSetListTable();	
		void setCreateAnnotationSetListTable(boolean value);
		
		@Description("Users can force update in case the table already exists.")
		@Default.Boolean(false)
		boolean getForceUpdate();	
		void setForceUpdate(boolean value);

		@Description("User can specify column separator.")
		@Default.String("\t")
		String getColumnSeparator();	 
		void setColumnSeparator(String ColumnSeparator);
		
		@Description("This provides whether the file contains POS field [true] instead of Start and End [false].")
		@Default.Boolean(false)
		boolean getPOS();
		void setPOS(boolean POS);
		
		@Description("This is for tranferring dataset file from the Cache repositority to Main repository.")
		@Default.Boolean(false)
		boolean getMoveToMainRepository();
		void setMoveToMainRepository(boolean MOVE);
	
		@Description("This is for the address of the main repository.")
		@Default.String("")
		String getMainRepositoryAddr();
		void setMainRepositoryAddr(String MainRepositoryAddr);
		
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

		if(!options.getColumnOrder().isEmpty()) {
			checkColumnOrder(options.getColumnOrder());
		}
		
		if(options.getMoveToMainRepository()) {
			if(options.getMainRepositoryAddr().isEmpty()) {
				throw new IllegalArgumentException("MoveToMainRepository needs the address of Main Repository (MainRepository); provide --mainRepositoryAddr option");
			}
		}

		try {
		
			if(checkTableExist()) {
				if (!options.getForceUpdate()) {
					LOG.warning(options.getBigQueryAnnotationSetTableId() + "  table already exists! If want to replace it, please set the option forceUpdate");
					return;
				}else {
					BigQueryFunctions.deleteTable(options.getBigQueryDatasetId(), options.getBigQueryAnnotationSetTableId());
				}
			}
			
			LOG.warning(options.getBigQueryAnnotationSetTableId() + "  is a new annotation table!");
		
			String [] inputFields = options.getHeader().split(",");
			if (VariantAnnotation){
				System.out.println("Variant Annotation Pipeline");
				p.apply(TextIO.read().from(options.getAnnotationInputTextBucketAddr()))
				.apply(ParDo.of(new FormatFn(inputFields,VariantAnnotation,baseStatus, options.getColumnOrder(), options.getColumnSeparator(), options.getPOS())))
				.apply(
			            BigQueryIO.writeTableRows()
			                .to(getTable(options.getProject(), options.getBigQueryDatasetId(), options.getBigQueryAnnotationSetTableId()))
			                .withSchema(getVariantSchema(options.getColumnOrder()))
			                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
			                .withWriteDisposition(WriteDisposition.WRITE_APPEND));
			}
			else{ //Generic
				System.out.println("Generic Annotation Pipeline");
	
				p.apply(TextIO.read().from(options.getAnnotationInputTextBucketAddr()))
				.apply(ParDo.of(new FormatFn(inputFields,VariantAnnotation,baseStatus, options.getColumnOrder(), options.getColumnSeparator(), options.getPOS())))
				.apply(
			            BigQueryIO.writeTableRows()
			                .to(getTable(options.getProject(), options.getBigQueryDatasetId(), options.getBigQueryAnnotationSetTableId()))
			                .withSchema(getTranscriptSchema(options.getColumnOrder()))
			                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
			                .withWriteDisposition(WriteDisposition.WRITE_APPEND));			
			}
	
				State state = p.run().waitUntilFinish();
				
				   if(options.getMoveToMainRepository()) {
					   if(state == State.DONE) {
	                     String command = "gsutil mv " + options.getAnnotationInputTextBucketAddr()
	                     + " " + options.getMainRepositoryAddr();

	                     Process p;
	                     p = Runtime.getRuntime().exec(command);
	                     p.waitFor();

	                     command = "gsutil mv " + options.getAnnotationInputTextBucketAddr()
	                     + ".md5sum " + options.getMainRepositoryAddr() + " ";

	                     Process p1;
	                     p1 = Runtime.getRuntime().exec(command);
	                     p1.waitFor();

	                     LOG.info("Successfully moved the input file to the main repository " + command);                          
				     }
				   }
				////////////////////////////////// End TIMER /////////////////////////////////////  
				long tempEstimatedTime = System.currentTimeMillis() - startTime;
				addToAnnotationSetList(tempEstimatedTime);		
		}
		catch (Exception e) {			
			LOG.severe(e.getMessage());
		}
		
	}
	
	private static boolean checkTableExist() {
		    BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();
		    BigQuery bigquery = optionsBuilder.build().getService();
		    LOG.warning("DatasetId: " + options.getBigQueryDatasetId() +" TableId: " + options.getBigQueryAnnotationSetTableId() );
		    //return bigquery.getDataset(options.getBigQueryDatasetId()).get(options.getBigQueryAnnotationSetTableId()).exists();		    	    
		    return bigquery.getDataset(options.getBigQueryDatasetId()).get(options.getBigQueryAnnotationSetTableId()) != null;		    	    
	}

	private static void checkColumnOrder(String columnOrder) {
		  String[] Order = columnOrder.split(",");
		  for(String s:Order) {
			  for (char c : s.toCharArray())
			    {
			        if (!Character.isDigit(c)) 
			        		throw new IllegalArgumentException("Wrong numbers in column order!");
			    }
		  }
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
	    	fields.add(Field.of("Build", Field.Type.string()));
	    	fields.add(Field.of("AnnotationSetSize", Field.Type.string()));
	    	fields.add(Field.of("Info", Field.Type.string()));
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
		    rowContent.put("Build", "Test");
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
	    rowContent.put("Build", options.getAssemblyId());

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
	 * @param Order 
	 * @return schema 
	 */
	
	static TableSchema getVariantSchema(String ColumnOrder) {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("chrm").setType("STRING"));
		fields.add(new TableFieldSchema().setName("start").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("end").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("base").setType("STRING"));
		fields.add(new TableFieldSchema().setName("alt").setType("STRING"));
		
		int chromIndex=0, startIndex=1, endIndex=2, refIndex=3, altIndex=4;
		
		if (!ColumnOrder.isEmpty()) {
				String[] order = ColumnOrder.split(",");
				
				if (order.length!=5) {
					throw new IllegalArgumentException("Column Order for Variant Annotation must contains the first "
							+ "five elements order (e.g., chrom,start,end,refBase,altBase=\"5,6,3,9,10\")");
				}
				
				chromIndex=Integer.parseInt(order[0])-1;
				startIndex=Integer.parseInt(order[1])-1;
				endIndex=Integer.parseInt(order[2])-1;
				refIndex=Integer.parseInt(order[3])-1;
				altIndex=Integer.parseInt(order[4])-1;
		}
			
		if (ColumnOrder.isEmpty()) {
			String[] inputFields= options.getHeader().split(",");
			for(int i=5; i<inputFields.length; i++){
				fields.add(new TableFieldSchema().setName(inputFields[i]).setType("STRING"));
			}
		}
		else {
			String[] inputFields= options.getHeader().split(",");
			for(int index=0; index<inputFields.length; index++){
				if(index!=chromIndex && index!=startIndex 
				&& index!=endIndex && index!=refIndex && index!=altIndex)
					fields.add(new TableFieldSchema().setName(inputFields[index]).setType("STRING"));
			}			
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
	
	static TableSchema getTranscriptSchema(String ColumnOrder) {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("chrm").setType("STRING"));
		fields.add(new TableFieldSchema().setName("start").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("end").setType("INTEGER"));
		
		int chromIndex=0, startIndex=1, endIndex=2;
		
		if (!ColumnOrder.isEmpty()) {
			String[] order = ColumnOrder.split(",");
			
			if (order.length!=3) {
				throw new IllegalArgumentException("Column Order for Generic Annotation must contains the first "
						+ "three elements order (e.g., chrom,start,end=\"5,6,3,9,10\")");
			}
			
			chromIndex=Integer.parseInt(order[0])-1;
			startIndex=Integer.parseInt(order[1])-1;
			endIndex=Integer.parseInt(order[2])-1;
		}
		
		if (ColumnOrder.isEmpty()) {

			String[] inputFields= options.getHeader().split(",");		
			for(int i=3; i<inputFields.length; i++){
				fields.add(new TableFieldSchema().setName(inputFields[i]).setType("STRING"));
			}
		}
		else {
			String[] inputFields= options.getHeader().split(",");
			for(int index=0; index<inputFields.length; index++){
				if(index!=chromIndex && index!=startIndex 
				&& index!=endIndex)
					fields.add(new TableFieldSchema().setName(inputFields[index]).setType("STRING"));
			}			
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
		private final boolean is_0_Based;
		private String columnOrder="";
		private String columnSeparator="";
		private final boolean POS;
		private static final long serialVersionUID = 7700800981719306804L;

		public FormatFn(String [] _inputFields, boolean _VA, boolean _zeroBased, String _columnOrder, String _columnSeparator, boolean _POS) {
			this.inputFields = _inputFields;
			this.VariantAnnotation = _VA;
			this.is_0_Based = _zeroBased;
			this.columnOrder=_columnOrder;
			this.columnSeparator=_columnSeparator;
			this.POS=_POS;
			
		}

		@org.apache.beam.sdk.transforms.DoFn.ProcessElement
		public void processElement(ProcessContext c) {
			for (String line : c.element().split("\n")) {
				if (!line.isEmpty() && !line.startsWith("#") && !line.startsWith("chrom")) {

					//UCSC \t is the only acceptable space b/w columns
					String[] vals = c.element().toString().split(this.columnSeparator);
					TableRow row = new TableRow();
					int chromIndex=0, startIndex=1, endIndex=2, refIndex=3, altIndex=4;
					
					//Variant
					if (this.VariantAnnotation){
						
						if (!this.columnOrder.isEmpty()) {
							String[] order = columnOrder.split(",");
							chromIndex=Integer.parseInt(order[0])-1;
							startIndex=Integer.parseInt(order[1])-1;
							endIndex=Integer.parseInt(order[2])-1;
							refIndex=Integer.parseInt(order[3])-1;
							altIndex=Integer.parseInt(order[4])-1;
						}
						
						row.set("chrm", canonicalizeRefName(vals[chromIndex]));
			
						//Our internal database representations of coordinates always 
						//have a zero-based start and a one-based end.
						if (!this.is_0_Based){
								row.set("start", Integer.parseInt(vals[startIndex])-1);
						}
						else{
							row.set("start", Integer.parseInt(vals[startIndex]));
						}
						
						int RefSize=0; 
						/*Make sure to handle special cases for reference bases [insertion]*/
						//For those cases that annotation reference file does not support refBase column 
						if (refIndex>=vals.length || vals[refIndex] == null || vals[refIndex].isEmpty() || vals[refIndex].equals("-"))
							row.set("base", "");
						else {
							row.set("base", vals[refIndex]);
							RefSize= vals[refIndex].length()-1;
						}
						
						if (this.columnOrder.isEmpty()) {						
							for (int index=5; index<vals.length; index++ )
								row.set(inputFields[index], vals[index]);
						}else {
							for (int index=0; index<vals.length; index++ )
								if(index!=chromIndex && index!=startIndex 
								&& index!=endIndex && index!=refIndex && index!=altIndex)
									row.set(inputFields[index], vals[index]);
						}
						
						//In case, there is no field for Start and End; then we need to calc End based on the number of ref bases 
						if (!this.POS)
							row.set("end", vals[endIndex]);
						else {
							row.set("end", Integer.parseInt(vals[endIndex]) + RefSize);
						}
						
						
						/*Make sure to handle special cases for alternate bases [deletion] */
						if (vals[altIndex] == null || vals[altIndex].isEmpty() || vals[altIndex].equals("-") 
								|| vals[altIndex].equals(".")) {
							row.set("alt", "");
							c.output(row);
						}
						else {
							if (vals[altIndex].split("/").length>1) {
								String[] alts = vals[altIndex].split("/");
								for(String allele : alts) {
									if (allele == null || allele.isEmpty() || allele.equals("-"))
										row.set("alt", "");
									else
										row.set("alt", allele);
									
									c.output(row);
								}
							}else {
								if (vals[altIndex] == null || vals[altIndex].isEmpty() || vals[altIndex].equals("-") || vals[altIndex].equals("."))
									row.set("alt", "");
								else
									row.set("alt", vals[altIndex]);
								c.output(row);
							}
						}
					}
					else{ //Generic+Transcript

						if (!this.columnOrder.isEmpty()) {
							String[] order = columnOrder.split(",");
							chromIndex=Integer.parseInt(order[0])-1;
							startIndex=Integer.parseInt(order[1])-1;
							endIndex=Integer.parseInt(order[2])-1;
						}
						
						row.set("chrm", canonicalizeRefName(vals[chromIndex]));
						
						if (!this.is_0_Based){
							row.set("start", Integer.parseInt(vals[startIndex])-1);
						}
						else{
							row.set("start", Integer.parseInt(vals[startIndex]));
						}
						
						row.set("end", vals[endIndex]);				

						
						if (this.columnOrder.isEmpty()) {						
							for (int index=3; index<vals.length; index++ )
								row.set(inputFields[index], vals[index]);
						}else {
							for (int index=0; index<vals.length; index++ )
								if(index!=chromIndex && index!=startIndex 
								&& index!=endIndex)
									row.set(inputFields[index], vals[index]);
						}
								
						c.output(row);						
					}
					
				}
			}
		}
	}

}

