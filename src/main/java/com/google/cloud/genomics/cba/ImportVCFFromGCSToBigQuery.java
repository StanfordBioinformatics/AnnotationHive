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
import java.util.concurrent.TimeoutException;
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
 * <h1> Import VCF Files</h1> This class
 * imports VCF from a Google bucket to BigQuery. It prepares and converts
 * VCF fields to the corresponding fields into a Google BigQuery table based on 
 * the provided header.
 * 
 * @version 1.0
 * @since 2018-06-01
 */

public class ImportVCFFromGCSToBigQuery {

	
	/*
	 * Options required to run this pipeline.
	 * 
	 * @param BigQueryDataset The ID of the Google BigQuery Dataset name that the output
	 * VCF will be posted.
	 * 
	 * @param VCFInputTextBucketAddr This provides the URI of inputfile
	 * which contains VCF file.
	 * 
	 * @param base0 This provides whether the reference VCF are 0-Based
	 * or 1-Based.
	 */
	public static interface Options extends GenomicsOptions {

		@Description("This provides the URI of inputfile which contains VCF. This is a required field.")
		@Default.String("")
		String getVCFInputTextBucketAddr();
		void setVCFInputTextBucketAddr(String VCFInputTextBucketAddr);

		@Description("This provides whether the base is 0 or 1. This is a required field.")
		@Default.String("")
		String getBase0();
		void setBase0(String base0);

		@Description("This provides the header for the VCF file. This is a required field. "
				+ "(It must start w/ the following fields: \"reference_name, start, end, reference_bases, alternate_bases other fileds. Otherwise, set the columnOrder option. \")")
		@Default.String("")
		String getHeader();
		void setHeader(String header);

		@Description("This provides BigQuery Dataset ID.")
		@Default.String("")
		String getBigQueryDatasetId();
		void setBigQueryDatasetId(String BigQueryDatasetId);

		@Description("This provides BigQuery Table.")
		@Default.String("")
		String getBigQueryVCFTableId();
		void setBigQueryVCFTableId(String BigQueryVCFTableId);
	
		@Description("This provides the version VCF. This is a required field.")
		@Default.String("")
		String getVCFVersion();	
		void setVCFVersion(String VCFVersion);
			
		@Description("This provides more info about the VCF. This is an optional filed.")
		@Default.String("")
		String getVCFInfo();	
		void setVCFInfo(String VCFInfo);

		@Description("This provides assemblyId. This is a required field.")
		@Default.String("")
		String getAssemblyId();	
		void setAssemblyId(String AssemblyId);

		@Description("This provides columnOrder for the first 5 columns <reference_name, start, end, reference_bases, alternate_bases>. This is an optional field.")
		@Default.String("")
		String getColumnOrder();	
		void setColumnOrder(String ColumnOrder);
		
		@Description("This provides the number of workers. This is a required filed.")
		int getNumWorkers();	
		void setNumWorkers(int value);

		@Description("This provides whether VCFList table exist or not. If not it will create the table.")
		@Default.Boolean(false)
		boolean getCreateVCFListTable();	
		void setCreateVCFListTable(boolean value);
		
		@Description("Users can force update in case the table already exists.")
		@Default.Boolean(false)
		boolean getForceUpdate();	 
		void setForceUpdate(boolean value);

		@Description("User can specify column separator.")
		//@Default.String("\\s+")
		@Default.String("\t")
		String getColumnSeparator();	
		void setColumnSeparator(String ColumnSeparator);
		
		@Description("This provides whether the file contains POS field [true] instead of Start and End [false].")
		@Default.Boolean(false)
		boolean getPOS();
		void setPOS(boolean POS);
		
		@Description("To find number of samples presenting a varaint. e.g., numSampleIDs=ABC1,ABC2,ABC3")
		@Default.String("")
		String getSampleIDs();
		void setSampleIDs(String SampleIDs);
		
	}

	private static Options options;
	private static Pipeline p;
	private static boolean DEBUG;
	private static final Logger LOG = Logger.getLogger(ImportVCFFromGCSToBigQuery.class.getName());

	public static void run(String[] args) throws GeneralSecurityException, IOException {


		////////////////////////////////// START TIMER /////////////////////////////////////  
		long startTime = System.currentTimeMillis();
		
		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		
		p = Pipeline.create(options);
		
		if (options.getBigQueryDatasetId().isEmpty()) {
			throw new IllegalArgumentException("BigQuery DatasetId must be specified");
		}

		if (options.getCreateVCFListTable()) {
			createVCFList();
			return;		
		}

		if (options.getBigQueryVCFTableId().isEmpty()) {
			throw new IllegalArgumentException("BigQuery VCF TableId must be specified");
		}
		
		if (options.getVCFInputTextBucketAddr().isEmpty()) {
			throw new IllegalArgumentException("VCFInputTextBucketAddr must be specified");
		}

		if (options.getVCFVersion().isEmpty()) {
			throw new IllegalArgumentException("VCF Version must be specified");
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
		
		if(!checkVCFTableExist()) {
			LOG.severe("Cannot Find VCFTable!");
			return;
		}
	

		if(!options.getColumnOrder().isEmpty()) {
			checkColumnOrder(options.getColumnOrder());
		}
		
		if(checkTableExist()) {
			if (!options.getForceUpdate()) {
				LOG.severe(options.getBigQueryVCFTableId() + "  table already exists! If want to replace it, please set the option forceUpdate");
				return;
			}else {
				BigQueryFunctions.deleteTable(options.getBigQueryDatasetId(), options.getBigQueryVCFTableId());
			}
		}
		else{
			LOG.warning(options.getBigQueryVCFTableId() + "  is a new VCF table!");
		}
		
	
		String [] inputFields = options.getHeader().split(",");
		LOG.warning("VCF Pipeline");
		LOG.warning("inputFields Size: " + inputFields.length );
		LOG.warning("inputFields: " + inputFields.toString());

			p.apply(TextIO.read().from(options.getVCFInputTextBucketAddr()))
			.apply(ParDo.of(new FormatFn(inputFields,baseStatus, options.getColumnOrder(), options.getColumnSeparator(), options.getPOS())))
			.apply(
		            BigQueryIO.writeTableRows()
		                .to(getTable(options.getProject(), options.getBigQueryDatasetId(), options.getBigQueryVCFTableId()))
		                .withSchema(getVCFSchema(options.getColumnOrder()))
		                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
		                .withWriteDisposition(WriteDisposition.WRITE_APPEND));
		
		try {
			State state = p.run().waitUntilFinish();
			////////////////////////////////// End TIMER /////////////////////////////////////  
			long tempEstimatedTime = System.currentTimeMillis() - startTime;
			addToVCFList(tempEstimatedTime);	
			
			if (!options.getSampleIDs().isEmpty()) {
				if(state == State.DONE) {
					LOG.warning("AnnotationHive assumed the imported VCF file has these fileds ID, QUAL, FILTER, INFO, FORMAT");					
					BigQueryFunctions.findNumSamples_StandardSQL(options.getProject(), 
							options.getBigQueryDatasetId(), options.getBigQueryVCFTableId(), 
							options.getSampleIDs(), options.getHeader());
					
				}
			}
			
			
		}
		catch (Exception e) {			
			LOG.severe(e.getMessage());

		}	
				
	}
	
	private static boolean checkTableExist() {
		    BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();
		    BigQuery bigquery = optionsBuilder.build().getService();
		    LOG.warning("DatasetId: " + options.getBigQueryDatasetId() +" TableId: " + options.getBigQueryVCFTableId() );
		    return bigquery.getDataset(options.getBigQueryDatasetId()).get(options.getBigQueryVCFTableId()) != null;		    	    
	}
	
	private static boolean checkVCFTableExist() {
	    BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();
	    BigQuery bigquery = optionsBuilder.build().getService();
	    LOG.warning("DatasetId: " + options.getBigQueryDatasetId() +" TableId: " + "VCFList" );
	    return bigquery.getDataset(options.getBigQueryDatasetId()).get("VCFList") != null;		    	    
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
	 * <h1>This function add a new table called VCFList.
	 * VCFList is a table containing 
	 * the information about VCF files. 
	 *
	 */	  
	public static void createVCFList() {
		
	    BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();
	    BigQuery bigquery = optionsBuilder.build().getService();

	    Table existTable = bigquery.getTable(options.getBigQueryDatasetId(), "VCFList");

	    if(existTable==null){	    	
	    	List<Field> fields = new ArrayList<Field>();
	    	fields.add(Field.of("VCFName", Field.Type.string()));
	    	fields.add(Field.of("VCFVersion", Field.Type.string()));
	    	fields.add(Field.of("VCFFields", Field.Type.string()));
	    	fields.add(Field.of("CreationDate", Field.Type.string()));
	    	fields.add(Field.of("Build", Field.Type.string()));
	    	fields.add(Field.of("VCFSize", Field.Type.string()));
	    	fields.add(Field.of("Info", Field.Type.string()));
			Schema schema = Schema.of(fields);		
			TableId tableId = TableId.of(options.getBigQueryDatasetId(), "VCFList");

			TableDefinition tableDefinition = StandardTableDefinition.of(schema);
			TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
			bigquery.create(tableInfo);
		    System.out.println("### Successfully created "+options.getProject() +":"+  options.getBigQueryDatasetId() + ".VCFList table.");

		    Map<String, Object> rowContent = new HashMap<>();
		    rowContent.put("VCFName", "Test");
		    rowContent.put("VCFVersion", "Test");
		    rowContent.put("VCFFields", "Test");
		    rowContent.put("CreationDate", "Test");
		    rowContent.put("Build", "Test");
		    rowContent.put("VCFSize", "Test");
		    rowContent.put("Info", "Test");
		    InsertAllResponse response;
	    		response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId).addRow("rowId", rowContent).build());
		    if(DEBUG){
		    		LOG.warning(response.toString());
		    }
	    }
	    else
	    		LOG.severe("### Table \"VCFList\" exists");
	}

	
	/**
	 * <h1>This function adds information about the VCF file 
	 * to the VCFList table. VCFList is a table containing 
	 * the information about VCF file 
	 *
	 */	
	static void addToVCFList(long tempEstimatedTime) {
	
	    BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();
	    BigQuery bigquery = optionsBuilder.build().getService();

	    
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
	    TableId tableId = TableId.of(options.getBigQueryDatasetId(), "VCFList");
	    Map<String, Object> rowContent = new HashMap<>();
	    rowContent.put("VCFName", options.getBigQueryVCFTableId());
	    rowContent.put("VCFVersion", options.getVCFVersion());
	    rowContent.put("VCFFields", options.getHeader());
	    rowContent.put("CreationDate", dateFormat.format(date).toString());
	    rowContent.put("Build", options.getAssemblyId());

		QueryResponse rs = BigQueryFunctions.runquery(BigQueryFunctions.countQuery(options.getProject(), options.getBigQueryDatasetId(), options.getBigQueryVCFTableId()));
		QueryResult result = rs.getResult();
		String 	numVariants="0";
		if (result != null)
			for (List<FieldValue> row : result.iterateAll()) {
				numVariants=row.get(0).getValue().toString();
		}
		
		LOG.info("### number of variants added: " + numVariants);
		rowContent.put("VCFSize", numVariants);
		if(options.getNumWorkers()!=0)
			rowContent.put("Info", options.getVCFInfo() + "\t Number of Instances: " + options.getNumWorkers() + " ExecutionTime (msec): " + tempEstimatedTime);
		else
			rowContent.put("Info", options.getVCFInfo() + "\t Number of Instances: AutoScaling ExecutionTime (msec): " + tempEstimatedTime);

	    
	    InsertAllResponse response;
	    response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId).addRow(rowContent).build());
	    
	    System.out.println(response.toString());
	    if(!response.getInsertErrors().isEmpty())
	    		LOG.severe("### Error in inserting a new table to VCFList Table: " + response.getInsertErrors().toString());
	    else
	    		LOG.info("### Successfully added a new VCF table: " + rowContent.toString());
	}

	/**
	 * <h1>This function defines the BigQuery table schema 
	 * used for variant VCF tables.
	 *  (e.g., 'reference_name', 'start', 'end', 'reference_bases', 'alternate_bases', ...)
	 * @param Order 
	 * @return schema 
	 */
	
	static TableSchema getVCFSchema(String ColumnOrder) {
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("reference_name").setType("STRING"));
		fields.add(new TableFieldSchema().setName("start_position").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("end_position").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("reference_bases").setType("STRING"));
		fields.add(new TableFieldSchema().setName("alternate_bases").setType("STRING"));//.setMode("REPEATED"));
		
		int chromIndex=0, startIndex=1, endIndex=2, refIndex=3, altIndex=4;
		
		if (!ColumnOrder.isEmpty()) {
				String[] order = ColumnOrder.split(",");
				
				if (order.length!=5) {
					throw new IllegalArgumentException("Column Order for VCF must contains the first "
							+ "five elements order (e.g., reference_name,start,end,reference_bases,alternate_bases=\"5,6,3,9,10\")");
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
	 * @param is_0_Base Define whether the VCF is 1-base (False) or 0-base (True)  
	 * @return row formatted TableRow
	 */

	static class FormatFn extends DoFn<String, TableRow> {

		private final String [] inputFields;
		private final boolean is_0_Based;
		private String columnOrder="";
		private String columnSeparator="";
		private final boolean POS;
		private static final long serialVersionUID = 7700800981719306804L;

		public FormatFn(String [] _inputFields, boolean _zeroBased, String _columnOrder, String _columnSeparator, boolean _POS) {
			this.inputFields = _inputFields;
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
					
					//VCF						
						if (!this.columnOrder.isEmpty()) {
							String[] order = columnOrder.split(",");
							chromIndex=Integer.parseInt(order[0])-1;
							startIndex=Integer.parseInt(order[1])-1;
							endIndex=Integer.parseInt(order[2])-1;
							refIndex=Integer.parseInt(order[3])-1;
							altIndex=Integer.parseInt(order[4])-1;
						}
						
						row.set("reference_name", canonicalizeRefName(vals[chromIndex]));
			
						//Our internal database representations of coordinates always 
						//have a zero-based start and a one-based end.
						if (!this.is_0_Based){
								row.set("start_position", Integer.parseInt(vals[startIndex])-1);
						}
						else{
							row.set("start_position", Integer.parseInt(vals[startIndex]));
						}
						
						int RefSize=0; 
						/*Make sure to handle special cases for reference bases [insertion]*/
						//For those cases that VCF reference file does not support refBase column 
						if (refIndex>=vals.length || vals[refIndex] == null || vals[refIndex].isEmpty() || vals[refIndex].equals("-"))
							row.set("reference_bases", "");
						else {
							row.set("reference_bases", vals[refIndex]);
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
							row.set("end_position", vals[endIndex]);
						else {
							row.set("end_position", Integer.parseInt(vals[endIndex]) + RefSize);
						}
						
						/*Our focus in this version is performance not storage*/
						//List<String> listAlt = new ArrayList<String>();
						/*Make sure to handle special cases for alternate bases [deletion] */
						if (vals[altIndex] == null || vals[altIndex].isEmpty() || vals[altIndex].equals("-")) {
							row.set("alternate_bases", "");
							c.output(row);
							//listAlt.add("");
						}
						else {
							if (vals[altIndex].split("/").length>1) {
								String[] alts = vals[altIndex].split("/");
								for(String allele : alts) {
									if (allele == null || allele.isEmpty() || allele.equals("-"))
										row.set("alternate_bases", "");
										//listAlt.add("");
									else
										row.set("alternate_bases", allele);
										//listAlt.add(allele);									
									c.output(row);
								}
							} else if (vals[altIndex].split(",").length>1) {
								String[] alts = vals[altIndex].split(",");
								for(String allele : alts) {
									if (allele == null || allele.isEmpty() || allele.equals("-"))
										row.set("alternate_bases", "");
										//listAlt.add("");
									else
										row.set("alternate_bases", allele);
										//listAlt.add(allele);									
									c.output(row);
								}
							} else {
								if (vals[altIndex] == null || vals[altIndex].isEmpty() || vals[altIndex].equals("-"))
									row.set("alternate_bases", "");
									//listAlt.add("");
								else
									row.set("alternate_bases", vals[altIndex]);
									//listAlt.add(vals[altIndex]);
								c.output(row);
							}
						}
						//row.set("alternate_bases", listAlt.toArray());
						//c.output(row);

					}
			}
		}
	}

}


