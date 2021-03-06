package com.google.cloud.genomics.cba;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.TableId;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;


public class BigQueryFunctions {
	private static final Logger LOG = Logger.getLogger(BigQueryAnnotateVariants.class.getName());

	public static boolean DEBUG = false;
	static Query query = new Query();

	
	/**
	 * This function receives the intermediate table ID and Bin Size and run OrderBy over 
	 * each interval and write the sorted result into an output file.
	 * 
	 * @param projectId The ID of Project
	 * @param datasetId The BigQuery Dataset ID (e.g., annotation)
	 * @param tableId  The ID of intermediate table
	 * @param outputFile The address and filename of output file (e.g., /tmp/output.VCF)      
	 * @param BinSize The size of intervals (e.g., default is 100000) 
	 */
	
	public static void sortByBin(String projectId, String datasetId, String tableId, String outputFile, int BinSize)
			throws Exception {


		String queryStat = "SELECT *, LENGTH(innerQ.Chromosome) as len from (SELECT  reference_name as Chromosome, "
				+ "count(*) as Cnt, min (start_position) as Minimum, max(start_position) as Maximum FROM   " + "[" + projectId + ":"
				+ datasetId + "." + tableId + "] "
				+ "group by Chromosome  order by Chromosome) as innerQ  order by len, innerQ.Chromosome";

		
		// Get the results.
		System.out.println("Stat Query: " + queryStat);
		QueryResponse response = runquery(queryStat);
		QueryResult result = response.getResult();

		// Per each chromosome, sort the result.
		List<FieldValue> X = null, Y = null, M = null;
		while (result != null) {
			for (List<FieldValue> row : result.iterateAll()) {
				String reference_name = row.get(0).getValue().toString();
				if (!reference_name.equals("M") && !reference_name.equals("MT") && !reference_name.equals("X") && !reference_name.equals("Y")) {
					System.out.println(reference_name + " " + row.get(1).getValue().toString() + " "
							+ row.get(2).getValue().toString() + " " + row.get(3).getValue().toString());
					partition(row.get(0).getValue().toString(),
							Integer.parseInt(row.get(1).getValue().toString()),
							Integer.parseInt(row.get(2).getValue().toString()),
							Integer.parseInt(row.get(3).getValue().toString()), projectId, datasetId, tableId,
							outputFile, BinSize);
				} else {
					if (reference_name.equals("M") || reference_name.equals("MT"))
						M = row;
					else if (reference_name.equals("X"))
						X = row;
					else if (reference_name.equals("Y"))
						Y = row;
				}
			}
			result = result.getNextPage();
		}
		if (X != null) {
			System.out.println(X.get(0).getValue().toString() + " " + X.get(1).getValue().toString() + " "
					+ X.get(2).getValue().toString() + " " + X.get(3).getValue().toString());
//			partition("chr" + X.get(0).getValue().toString(), Integer.parseInt(X.get(1).getValue().toString()),
//					Integer.parseInt(X.get(2).getValue().toString()), Integer.parseInt(X.get(3).getValue().toString()),
//					projectId, datasetId, tableId, outputFile, BinSize);
			
			partition( X.get(0).getValue().toString(), Integer.parseInt(X.get(1).getValue().toString()),
					Integer.parseInt(X.get(2).getValue().toString()), Integer.parseInt(X.get(3).getValue().toString()),
					projectId, datasetId, tableId, outputFile, BinSize);

		}
		if (Y != null) {
			System.out.println(Y.get(0).getValue().toString() + " " + Y.get(1).getValue().toString() + " "
					+ Y.get(2).getValue().toString() + " " + Y.get(3).getValue().toString());
			partition(Y.get(0).getValue().toString(), Integer.parseInt(Y.get(1).getValue().toString()),
					Integer.parseInt(Y.get(2).getValue().toString()), Integer.parseInt(Y.get(3).getValue().toString()),
					projectId, datasetId, tableId, outputFile, BinSize);
//			partition("chr" + Y.get(0).getValue().toString(), Integer.parseInt(Y.get(1).getValue().toString()),
//					Integer.parseInt(Y.get(2).getValue().toString()), Integer.parseInt(Y.get(3).getValue().toString()),
//					projectId, datasetId, tableId, outputFile, BinSize);

		}
		if (M != null) {
			System.out.println(M.get(0).getValue().toString() + " " + M.get(1).getValue().toString() + " "
					+ M.get(2).getValue().toString() + " " + M.get(3).getValue().toString());
			partition(M.get(0).getValue().toString(), Integer.parseInt(M.get(1).getValue().toString()),
					Integer.parseInt(M.get(2).getValue().toString()), Integer.parseInt(M.get(3).getValue().toString()),
					projectId, datasetId, tableId, outputFile, BinSize);
			//partition("chr" + M.get(0).getValue().toString(), Integer.parseInt(M.get(1).getValue().toString()),
			//Integer.parseInt(M.get(2).getValue().toString()), Integer.parseInt(M.get(3).getValue().toString()),
			//projectId, datasetId, tableId, outputFile, BinSize);

		}
	}



	
	private static void partition(String chromId, int count, int startMin, int startMax, String projectId,
			String datasetId, String tableId, String outputFile, int BinSize) {

		
		//If count is 0, it wouldn't even reach here
		if(BinSize < count && count>0)
			 for (int interval = startMin; interval<= startMax; interval += BinSize){
					sortAndPrint (chromId, count, interval, interval+BinSize, projectId, datasetId, tableId, outputFile, BinSize );
			 }//END of FOR LOOP
		else
			sortAndPrint (chromId, count, startMin, startMax, projectId, datasetId, tableId, outputFile, BinSize );
		
	}

	
	private static void sortAndPrint(String chromId, int count, int startMin, int startMax, String projectId,
			String datasetId, String tableId, String outputFile, int BinSize){
		
		
		String queryStat = "SELECT  * from " + "[" + projectId + ":" + datasetId + "." + tableId + "] "
				+ "where reference_name='" + chromId + "' and "
				 + startMin + "<= start_position and start_position <= " 
				 + startMax 
				+ " order by reference_name, start_position";
		//if (DEBUG)
			System.out.println(queryStat);
		// Get the results.
		QueryResponse response = runquery(queryStat);
		QueryResult result = response.getResult();
		if (result != null) {
			List<FieldValue> oldRow=null;
			for (List<FieldValue> row : result.iterateAll()) {
				try {
					 String temp="";
				      for (FieldValue val : row) {
				    	  if(!val.isNull())
				    		  temp += val.getValue().toString() + "\t";
				        }
				        //System.out.printf("\n");
				    FileWriter fw = new FileWriter(outputFile, true);    
				    //fw.write(row.get(0).getValue().toString() + "\n");
				    
				    if(oldRow==null)				    
				    //	fw.write(temp + "\n");
				    	fw.write(temp);
				    else{
					    	if (row.get(0).toString().equals(oldRow.get(0).toString()) &&
					    			row.get(1).toString().equals(oldRow.get(1).toString()) && 
					    			row.get(2).toString().equals(oldRow.get(2).toString())){
		
						    		String SameRow="";
								      for (int index=5; index<row.size(); index++) {
								    	  FieldValue val= row.get(index);
								    	  if(!val.isNull())
								    		  SameRow += val.getValue().toString() + "\t";
								        }
	
								      fw.write(SameRow);
					    	}
					    	else{
						    	fw.write("\n");
						    	fw.write(temp);
					    	}
				    }
				    oldRow = row;
			    		//fw.write("\n");
					fw.close();
				} catch (IOException ioe) {
					System.err.println("IOException: " + ioe.getMessage());
				}

			}
		}

	}
	
	public static QueryResponse runquery(String querySql) {

		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(querySql).build();

		queryConfig.allowLargeResults();
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

		JobId jobId = JobId.of(UUID.randomUUID().toString());
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

		// Wait for the query to complete.
		try {
			queryJob = queryJob.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}

		System.out.print(queryJob);
		
		// Check for errors
		if (queryJob == null) {
			throw new RuntimeException("Job no longer exists");
		} else if (queryJob.getStatus().getError() != null) {
			// You can also look at queryJob.getStatus().getExecutionErrors()
			// for all
			// errors, not just the latest one.
			throw new RuntimeException(queryJob.getStatus().getError().toString());
		}

		return bigquery.getQueryResults(jobId);
	}

	public static void runQueryPermanentTable(String ProjectId, String queryString, String destinationDataset, String destinationTable,
			boolean allowLargeResults, int MaximumBillingTier, boolean LegacySql, boolean Update, boolean DDL) throws TimeoutException, InterruptedException {
		QueryJobConfiguration queryConfig;
		
		if (Update) {
			queryConfig = QueryJobConfiguration.newBuilder(queryString)
					.setUseLegacySql(LegacySql)
					.build();
		}
		else {
			if (MaximumBillingTier>1)
				queryConfig = QueryJobConfiguration.newBuilder(queryString)
					.setDestinationTable(TableId.of(destinationDataset, destinationTable))
					.setAllowLargeResults(allowLargeResults)
					.setMaximumBillingTier(MaximumBillingTier)
					.build();
			else {
				if (!DDL) {
					queryConfig = QueryJobConfiguration.newBuilder(queryString)
							.setDestinationTable(TableId.of(destinationDataset, destinationTable))
							.setAllowLargeResults(allowLargeResults)
							.setUseLegacySql(LegacySql)
							.build();
				}else {
					queryConfig = QueryJobConfiguration.newBuilder(queryString)
							.setUseLegacySql(LegacySql)
							.build();
				}
			}
		}
		queryConfig.allowLargeResults();
		//BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		//BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("gbsc-gcp-project-annohive-dev").build().getService(); //.getDefaultInstance().getService();
		BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(ProjectId).build().getService(); 
		
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

		// Wait for the query to complete.
		try {
			queryJob = queryJob.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}

		// Check for errors
		if (queryJob == null) {
			throw new RuntimeException("Job no longer exists");
		} else if (queryJob.getStatus().getError() != null) {
			// You can also look at queryJob.getStatus().getExecutionErrors()
			// for all
			// errors, not just the latest one.
			throw new RuntimeException(queryJob.getStatus().getError().toString());
		}
	}
		

	//TODO: for annotation tables, remove them from AnnotationList Table as well
	public static void deleteTable(String ProjectId, String destinationDataset, String destinationTable){
		//BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(ProjectId).build().getService(); //.getDefaultInstance().getService();

		Boolean deleted = bigquery.delete( destinationDataset,  destinationTable);
		if (deleted) {
		  // the table was deleted
			System.out.println("Successfully deleted the intermediate table: " + destinationDataset +"."+destinationTable);
		} else {
		  // the table was not found
			System.out.println("Failed to delete the intermediate table: " + destinationDataset +"."+destinationTable);
		}
	}
	
	public static String countQuery(String ProjectId, String datasetId, String TableId){
		String Query="";
			Query = "SELECT COUNT(*) from ["+ProjectId+":"+datasetId +"."+TableId+ "]";
		return Query;
	}

	public static String prepareAnnotateVariantQuerySQLStandard(String VCFTableNames, String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			  String TranscriptCanonicalizeRefNames, String VariantAnnotationTableNames, String VariantannotationCanonicalizerefNames) {

		String[] TranscriptAnnotations=null;
		String[] VariantAnnotationTables=null;
		String[] VCFTables=null;
		String[] VCFCanonicalize=null; 
		String[] TranscriptCanonicalize=null;
		String[] VariantannotationCanonicalize=null;
	
		/////////////////////Transcripts//////////////
		if(!TranscriptAnnotationTableNames.isEmpty()){
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
	
			if(!TranscriptCanonicalizeRefNames.isEmpty()){
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}
		////////////Variant Annotations///////
		if(!VariantAnnotationTableNames.isEmpty()){
			VariantAnnotationTables = VariantAnnotationTableNames.split(","); 
			
			if(!VariantannotationCanonicalizerefNames.isEmpty()){
				VariantannotationCanonicalize = VariantannotationCanonicalizerefNames.split(","); 
				if (VariantannotationCanonicalize.length != VariantAnnotationTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}

		}
		
		//////////VCF Files/////////////
		/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
		if(!VCFTableNames.isEmpty()){
			VCFTables = VCFTableNames.split(","); 
		
			if(!VCFCanonicalizeRefNames.isEmpty()){
				VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
			else{
				System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}
		
				
		
		/*#######################Prepare VCF queries#######################*/
		//TODO: Assumption is there is only one mVCF file
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index< VCFTables.length; index++){
			if(VCFCanonicalize!=null)
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, `end_position`, reference_bases, alt as alternate_bases ";

			VCFQuery += " FROM `"+ VCFTables[index]  +"`  AS v,  UNNEST(call) AS c, UNNEST(alternate_bases) AS alt   WHERE EXISTS ( SELECT alt FROM UNNEST(v.alternate_bases) alt WHERE alt NOT IN (\"<NON_REF>\", \"<*>\"))";
		}
		 VCFQuery +=" ) as VCF ";

		 	 
		String AllFields=""; // Use to 
		String AnnotationQuery="";
		int AnnotationIndex=1; // Use for creating alias names
		
		/*#######################Prepare VCF queries#######################*/
		if(VariantAnnotationTables!=null){
			for (int index=0; index< VariantAnnotationTables.length; index++,AnnotationIndex++){
						
						/* Example: gbsc-gcp-project-cba.PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
						 * [0] ProjectId: gbsc-gcp-project-cba
						 * [1] DatasetId: PublicAnnotationSets
						 * [2] TableId: hg19_GME
						 * [3] Features: GME_AF:GME_NWA:GME_NEA
						 */
				
						//gbsc-gcp-project-cba.AnnotationHiveSets.hg19_cosmic70:CosmicID
						String [] TableInfo = VariantAnnotationTables[index].split(":");
						//TableInfo[0] gbsc-gcp-project-cba.AnnotationHiveSets.hg19_cosmic70
						// TableInfo[2] CosmicID ...
						
						
						String RequestedFields="";
						String AliasTableName= "Annotation" + AnnotationIndex; 
						String TableName = TableInfo[0]; //+"."+TableInfo[1]; // was :
						
						for (int index_field=1; index_field<TableInfo.length; index_field++){
							if(index_field+1 > TableInfo.length)
								RequestedFields += AliasTableName +"." + TableInfo[index_field] + " , ";
							else 
								RequestedFields += AliasTableName +"." + TableInfo[index_field];
							
							if(index_field==1){ // this is for labeling the annotation record
								if(TableInfo.length>2)
									AllFields += "CONCAT(\""+ TableName.split("\\.")[2] +": \","+ AliasTableName +"." + TableInfo[index_field] + " ) , ";
								else
									AllFields += "CONCAT(\""+ TableName.split("\\.")[2] +": \","+ AliasTableName +"." + TableInfo[index_field] + " ) ";
							}
							else{
								if (index+1<TableInfo.length)
									AllFields += AliasTableName +"." + TableInfo[index_field] + " , ";
								else
									AllFields += AliasTableName +"." + TableInfo[index_field];
							}
						}
												
						AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, " + RequestedFields
							+ " FROM " + VCFQuery 
							+ " JOIN `" + TableName +"` AS " + AliasTableName ;
						AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start_position = VCF.start_position) AND (" + AliasTableName + ".end_position = VCF.end_position) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alternate_bases) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alternate_bases = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".reference_bases)))"; // , ";
			}
		}
		
		//TODO:Transcript + SQL Standard		
	
		String Query= "#standardSQL \n SELECT "
				+ " VCF.reference_name as reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, " + AllFields 
				+ " FROM ";
		
		Query += AnnotationQuery;
		//Query += " group by reference_name,  start,  end, reference_bases, alternate_bases, " + GroupByFields; 

		
		return Query;
	}
	
	public static String prepareAnnotateVariantQueryConcatFields_mVCF(String VCFTableNames, 
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			  String TranscriptCanonicalizeRefNames, String TranscriptAnnotationTableNames_wUCSC, String VariantAnnotationTableNames, 
			  String VariantannotationCanonicalizerefNames, 
			  boolean createVCF, boolean TempVCF, boolean GoogleVCF, boolean numSamples) {


		String[] TranscriptAnnotations_wUCSC=null;
		String[] TranscriptAnnotations=null;
		String[] VariantAnnotationTables=null;
		String[] VCFTables=null;
		String[] VCFCanonicalize=null; 
		String[] TranscriptCanonicalize=null;
		String[] VariantannotationCanonicalize=null;

		
		int numAnnotationDatasets=0;
		/////////////////////Transcripts_wUCSC//////////////
		if(TranscriptAnnotationTableNames_wUCSC!=null){
			TranscriptAnnotations_wUCSC = TranscriptAnnotationTableNames_wUCSC.split(","); 
			numAnnotationDatasets=+ TranscriptAnnotations_wUCSC.length;
		}

		
		/////////////////////Transcripts//////////////
		if(TranscriptAnnotationTableNames!=null){
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
			numAnnotationDatasets=+ TranscriptAnnotations.length;
			if(!TranscriptCanonicalizeRefNames.isEmpty()){
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
				
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}
		
		////////////Variant Annotations///////
		if(VariantAnnotationTableNames!=null){
			VariantAnnotationTables = VariantAnnotationTableNames.split(","); 
			numAnnotationDatasets=+ VariantAnnotationTables.length;

			if(!VariantannotationCanonicalizerefNames.isEmpty()){
				VariantannotationCanonicalize = VariantannotationCanonicalizerefNames.split(","); 
				if (VariantannotationCanonicalize.length != VariantAnnotationTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
		}
		
		
		//BigQuery Limit
		boolean GroupBy;
		if(numAnnotationDatasets<=64)
			GroupBy=true;
		else
			GroupBy=false;
		
		
		//////////VCF Files/////////////
		/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
		if(!VCFTableNames.isEmpty()){
			VCFTables = VCFTableNames.split(","); 
		
			if(!VCFCanonicalizeRefNames.isEmpty()){
				VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
			else{
				System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}
				
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index< VCFTables.length; index++){
			if(VCFCanonicalize!=null)
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, end_position, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, end_position, reference_bases, alternate_bases ";
			
			if (numSamples && GoogleVCF) {
				VCFQuery += ", count(*) AS num_samples ";
			}
			
			if (index+1<VCFTables.length) {
				if (GoogleVCF) { // Google VCF files -> filter out non-variants
					if (numSamples) // Skip homozygous reference calls
						VCFQuery += " FROM flatten(["+ VCFTables[index]  +"], call) ";
					else
						VCFQuery += " FROM ["+ VCFTables[index]  +"] ";
					//Skip no-calls
					VCFQuery += " OMIT RECORD IF EVERY(call.genotype <= 0)";
					if(numSamples) { 
						VCFQuery += 
							 " Group by reference_name," + 
							"  start_position," + 
							"  end_position," + 
							"  reference_bases," + 
							"  alternate_bases ";
					}
					VCFQuery += "), ";
				}else { //AnnotationHive VCF Template
					VCFQuery += " FROM ["+ VCFTables[index]  +"]), ";
				}
			}
			else {
				if (GoogleVCF) { // Google VCF files -> filter out non-variants
					if (numSamples) // Skip homozygous reference calls
						VCFQuery += " FROM flatten(["+ VCFTables[index]  +"], call) ";
					else
						VCFQuery += " FROM ["+ VCFTables[index]  +"] "; 		

					//Skip no-calls
					VCFQuery += " OMIT RECORD IF EVERY(call.genotype <= 0)";
					
					if(numSamples) { 
						VCFQuery += 
							 " Group by reference_name," + 
							"  start_position," + 
							"  end_position," + 
							"  reference_bases," + 
							"  alternate_bases ";
					}
					VCFQuery += ") ";
				}else { //AnnotationHive VCF Template
					VCFQuery += " FROM ["+ VCFTables[index]  +"]) ";
				}
			}
		}
		VCFQuery +=" ) as VCF ";

		 	 
		String AllFields=""; // Use to 
		String AnnotationQuery="";
		int AnnotationIndex=1; // Use for creating alias names
		
		/*#######################Prepare VCF queries#######################*/
		if(VariantAnnotationTables!=null){
			for (int index=0; index< VariantAnnotationTables.length; index++,AnnotationIndex++){
						
						/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
						 * ProjectId: gbsc-gcp-project-cba
						 * DatasetId: PublicAnnotationSets
						 * TableId: hg19_GME
						 * Features: GME_AF:GME_NWA:GME_NEA
						 */
						
						String [] TableInfo = VariantAnnotationTables[index].split(":");
												
						String RequestedFields="";
						String AliasTableName= "Annotation" + AnnotationIndex; 
						String TableName = TableInfo[0]+":"+TableInfo[1];
						
						
						for (int fieldIndex=2; fieldIndex<TableInfo.length; fieldIndex++){
							if (TableInfo.length>3){ // Creating CONCAT
								if (fieldIndex+1 < TableInfo.length) { // This will add "/" after fields => not the last field CONCAT(X, "/", Y, "/", Z) 
									if(createVCF)
										RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" , \"/\" ,";
									else 
										RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" ,";
								}
								else
									RequestedFields += AliasTableName +"." + TableInfo[fieldIndex];
							}
							else //If there is only one feature
								RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] ;
							
							if(fieldIndex==2 && createVCF){ //Special case where we add an index to the first field 
									if (TableInfo.length>3){ //Top Select 
										//(e.g.,     CONCAT(1: Annotation3.Annotation) 
											AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
									}
									else{//(e.g.,     CONCAT(1: Annotation3.name2) 
											AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
									}
								}
							else if (!createVCF) {// in case of creating Table  
								if (GroupBy)
									AllFields += ", max("+ AliasTableName +"." + TableInfo[fieldIndex] + ") as " + TableInfo[1] +"." + TableInfo[fieldIndex];
								else	
									AllFields += ", "+ AliasTableName +"." + TableInfo[fieldIndex] + " as " +  TableInfo[1] +"." + TableInfo[fieldIndex];
							}							
						}
						
						//IF the number of fields is more that 1 -> then concat all of them
						if (TableInfo.length>3) {
							
							AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases ";
							if (numSamples && GoogleVCF) {
								AnnotationQuery += ", num_samples ";
							}
							
							if(createVCF)
								AnnotationQuery += ", CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
							else 
								AnnotationQuery += ", " + RequestedFields ;

						}
						else {
							AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, ";
							if (numSamples && GoogleVCF) {
								AnnotationQuery += " num_samples, ";
							}
							AnnotationQuery +=  RequestedFields;
						}
						
						AnnotationQuery +=
							 " FROM " + VCFQuery 
							+ " JOIN [" + TableName +"] AS " + AliasTableName ;
						AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start_position = VCF.start_position) AND (" + AliasTableName + ".end_position = VCF.end_position) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alternate_bases) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alternate_bases = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".reference_bases)) ";
						
						//This is the case when we have transcript annotations 
						if(index+1 <  VariantAnnotationTables.length || (TranscriptAnnotations!=null || TranscriptAnnotations_wUCSC!=null))
							AnnotationQuery +=  "), ";
						else
							AnnotationQuery +=  ") ";
			}
		}
		
		 
		if(TranscriptAnnotations!=null){
			for (int index=0; index< TranscriptAnnotations.length; index++, AnnotationIndex++){

				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */
				String [] TableInfo = TranscriptAnnotations[index].split(":");
								
				String RequestedFields="";
				String AliasTableName= "Annotation" + AnnotationIndex; 
				String TableName = TableInfo[0]+":"+TableInfo[1];
				
				
				for (int fieldIndex=2; fieldIndex<TableInfo.length; fieldIndex++){
					if (TableInfo.length>3){ // Creating CONCAT
						if (fieldIndex+1 < TableInfo.length){
							if(createVCF)
								RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" , \"/\" ,";
							else
								RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" ,";
						}
						else
							RequestedFields += AliasTableName +"." + TableInfo[fieldIndex];
					}
					else //If there is only one feature
						RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] ;

								
					
					if(fieldIndex==2 && createVCF){
						
						int tempIndex=0; //alignment
						if(VariantAnnotationTables!=null) {
							tempIndex+=VariantAnnotationTables.length;
						}
						
						if (TableInfo.length>3){ //Top Select 
								
							AllFields += ", CONCAT(\""+ (index+1+tempIndex) +": \","+ AliasTableName +"." + AliasTableName + " ) ";

//							if(VariantAnnotationTables!=null) {
//									AllFields += ", CONCAT(\""+ (index+1+VariantAnnotationTables.length) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
//							}
//							else {
//									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
//							}
						}
						else{
							AllFields += ", CONCAT(\""+ (index+1+tempIndex) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";

//							if(VariantAnnotationTables!=null) {
//									AllFields += ", CONCAT(\""+ (index+1+VariantAnnotationTables.length) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
//								
//							}
//							else	 {
//									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";								
//							}
						}
					}	
					else if (!createVCF) {// in case of creating Table  
						if (GroupBy)
							AllFields += ", max("+ AliasTableName +"." + TableInfo[fieldIndex] + ") as " + TableInfo[1] +"." + TableInfo[fieldIndex];
						else	
							AllFields += ", "+ AliasTableName +"." + TableInfo[fieldIndex] +" as " + TableInfo[1] +"." + TableInfo[fieldIndex] ;
					}
				}

				//IF the number of fields is more that 1 -> then concat all of them
				if (TableInfo.length>3) {
					AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, "
					+ "VCF.alternate_bases ";

					if (numSamples && GoogleVCF) {
						AnnotationQuery += ", num_samples ";
					}
					
					if(createVCF)
						AnnotationQuery += ", CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
					else 
						AnnotationQuery += ", " + RequestedFields ;				
						//, CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
				}
				else {
					AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, ";
					if (numSamples && GoogleVCF) {
						AnnotationQuery += " num_samples, ";
					}	
					AnnotationQuery += RequestedFields;
				}
				AnnotationQuery +=
					 " FROM " + VCFQuery 
					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";


				AnnotationQuery += " WHERE "
						+ " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) ";				
				
				if(index+1 <  TranscriptAnnotations.length || TranscriptAnnotations_wUCSC!=null)
					AnnotationQuery +=  "), ";
				else
					AnnotationQuery +=  ") ";
			
			}
		} //end of Generic w/o UCSC
		
		if(TranscriptAnnotations_wUCSC!=null){
			for (int index=0; index< TranscriptAnnotations_wUCSC.length; index++, AnnotationIndex++){

				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */
				
				String [] TableInfo = TranscriptAnnotations_wUCSC[index].split(":");
								
				String RequestedFields="";
				String AliasTableName= "Annotation" + AnnotationIndex; 
				String TableName = TableInfo[0]+":"+TableInfo[1];
				
				
				for (int fieldIndex=2; fieldIndex<TableInfo.length; fieldIndex++){
					if (TableInfo.length>3){ // Creating CONCAT
						if (fieldIndex+1 < TableInfo.length){
							if(createVCF)
								RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" , \"/\" ,";
							else
								RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" ,";
						}
						else
							RequestedFields += AliasTableName +"." + TableInfo[fieldIndex];
					}
					else //If there is only one feature
						RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] ;

					if(fieldIndex==2 && createVCF){
						
						int tempIndex=0; //alignment
						if(VariantAnnotationTables!=null) {
							tempIndex+=VariantAnnotationTables.length;
						}
						if (TranscriptAnnotations!=null) {
							tempIndex+=TranscriptAnnotations.length;
						}
						if (TableInfo.length>3){ //Top Select for naming 
							
							AllFields += ", CONCAT(\""+ (index+1+tempIndex) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
						}
						else{
							AllFields += ", CONCAT(\""+ (index+1+tempIndex) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
						}
					}	
					else if (!createVCF) {// in case of creating Table  
						if (GroupBy)
							AllFields += ", max("+ AliasTableName +"." + TableInfo[fieldIndex] + ") as " + TableInfo[1] +"." + TableInfo[fieldIndex];
						else	
							AllFields += ", "+ AliasTableName +"." + TableInfo[fieldIndex] +" as " + TableInfo[1] +"." + TableInfo[fieldIndex] ;
					}
				}

				//IF the number of fields is more that 1 -> then concat all of them
				if (TableInfo.length>3) {
					AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, "
					+ "VCF.alternate_bases ";

					if (numSamples && GoogleVCF) {
						AnnotationQuery += ", num_samples ";
					}
					
					if(createVCF)
						AnnotationQuery += ", CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
					else 
						AnnotationQuery += ", " + RequestedFields ;				
						//, CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
				}
				else {
					AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, ";
					if (numSamples && GoogleVCF) {
						AnnotationQuery += " num_samples, ";
					}	
					AnnotationQuery += RequestedFields;
				}
				AnnotationQuery +=
					 " FROM " + VCFQuery 
					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
						+ "AND (\"" + AliasTableName + "\".UCSC_ID = VCF.UCSC_ID)";
				
				if(index+1 <  TranscriptAnnotations_wUCSC.length)
					AnnotationQuery +=  "), ";
				else
					AnnotationQuery +=  ") ";
			
			}
		}
	
		if (numSamples && GoogleVCF) {
			AllFields = ",  num_samples " + AllFields ;
		}
		String Query= "  SELECT "
				+ " VCF.reference_name as reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases as reference_bases, "
				+ "VCF.alternate_bases as alternate_bases " + AllFields 
				+ " FROM ";
		
		Query += AnnotationQuery;
		
		if(!createVCF && GroupBy) {
			Query += " GROUP BY  reference_name, start_position, end_position, reference_bases, alternate_bases "; 
			if (numSamples) {
				Query += ",  num_samples " ;
			}
		}
		return Query;
	}

	
	
	public static String prepareAnnotateVariantQueryWithSampleNames(String VCFTableNames, String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			  String TranscriptCanonicalizeRefNames, String VariantAnnotationTableNames, String VariantannotationCanonicalizerefNames, 
			  String SampleNames, boolean createVCF, boolean customizedVCF, boolean GoogleVCF) {

		String[] TranscriptAnnotations=null;
		String[] VariantAnnotationTables=null;
		String[] VCFTables=null;
		String[] VCFCanonicalize=null; 
		String[] TranscriptCanonicalize=null;
		String[] VariantannotationCanonicalize=null;
	
		/////////////////////Transcripts//////////////
		if(TranscriptAnnotationTableNames!=null){
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
	
			if(!TranscriptCanonicalizeRefNames.isEmpty()){
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}
		////////////Variant Annotations///////
		if(VariantAnnotationTableNames!=null){
			VariantAnnotationTables = VariantAnnotationTableNames.split(","); 
			
			if(!VariantannotationCanonicalizerefNames.isEmpty()){
				VariantannotationCanonicalize = VariantannotationCanonicalizerefNames.split(","); 
				if (VariantannotationCanonicalize.length != VariantAnnotationTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
		}
		
		//////////VCF Files/////////////
		/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
		if(!VCFTableNames.isEmpty()){
			VCFTables = VCFTableNames.split(","); 
		
			if(!VCFCanonicalizeRefNames.isEmpty()){
				VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
			else{
				System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}
		
				
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index < VCFTables.length; index++){
			if(VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, end_position ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, end_position ";

			if(VariantAnnotationTableNames!=null){
				VCFQuery += ", reference_bases, alternate_bases";
			}
			
			if(!SampleNames.isEmpty())
				VCFQuery +=  ", call.call_set_name  ";
			
			if (!customizedVCF && GoogleVCF) {
				if (index+1<VCFTables.length)
					VCFQuery += " FROM FLATTEN(["+ VCFTables[index]  +"], call.call_set_name) OMIT RECORD IF EVERY(call.genotype <= 0) ), ";
				else
					VCFQuery += " FROM FLATTEN(["+ VCFTables[index]  +"], call.call_set_name) OMIT RECORD IF EVERY(call.genotype <= 0) ) ";
			}else //CustomizedVCF
			{
					VCFQuery += " FROM ["+ VCFTables[index]  +"] )) as VCF ";
			}
		}
		if (!customizedVCF && GoogleVCF) {
			VCFQuery +=" where call.call_set_name = \""+ SampleNames + "\") as VCF ";
		}
		 	 
		String AllFields=""; // Use to 
		String AnnotationQuery="";
		int AnnotationIndex=1; // Use for creating alias names
		
		/*#######################Prepare VCF queries#######################*/
		if(VariantAnnotationTables!=null){
			for (int index=0; index< VariantAnnotationTables.length; index++,AnnotationIndex++){
						
						/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
						 * ProjectId: gbsc-gcp-project-cba
						 * DatasetId: PublicAnnotationSets
						 * TableId: hg19_GME
						 * Features: GME_AF:GME_NWA:GME_NEA
						 */
						
						String [] TableInfo = VariantAnnotationTables[index].split(":");
												
						String RequestedFields="";
						String AliasTableName= "Annotation" + AnnotationIndex; 
						String TableName = TableInfo[0]+":"+TableInfo[1];
						
						for (int index2=2; index2<TableInfo.length; index2++){
							RequestedFields +=  " , " + AliasTableName +"." + TableInfo[index2] ;
							if(index2==2 && createVCF){
//								if (GroupBy)
//									AllFields += ", MAX ( CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[index2] + " )) ";
//								else
									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[index2] + " ) ";
									//AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[index2] + " ) ";

								//CONCAT("hg19_cosmic70: ",Annotation1.CosmicID )
							}
							else{
								if (!createVCF)
									AllFields +=  " , MAX( " + AliasTableName +"." + TableInfo[index2] + ") as " + TableInfo[1].split("\\.")[1] +"_"+ TableInfo[index2];
								else
									AllFields +=  " , " + AliasTableName +"." + TableInfo[index2];
							}
						}
												
						AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases " + RequestedFields
							+ " FROM " + VCFQuery 
							+ " JOIN [" + TableName +"] AS " + AliasTableName ;
						AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start_position = VCF.start_position) AND (" + AliasTableName + ".end_position = VCF.end_position) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alternate_bases) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alternate_bases = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".reference_bases)) ";
									//+ "GROUP BY VCF.reference_name, VCF.start_position, VCF.END, VCF.reference_bases, VCF.alternate_bases " + RequestedFields;
						if(index+1 <  VariantAnnotationTables.length || (TranscriptAnnotations!=null))
							AnnotationQuery +=  "), "; //In case there are more variant annotations OR we have  
						else
							AnnotationQuery +=  ") ";
			}
		}
		
		 
		if(TranscriptAnnotations!=null){
				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */

			for (int index=0; index< TranscriptAnnotations.length; index++,AnnotationIndex++){
				
				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */
				
				String [] TableInfo = TranscriptAnnotations[index].split(":");
										
				String RequestedFields="";
				String AliasTableName= "Annotation" + AnnotationIndex; 
				String TableName = TableInfo[0]+":"+TableInfo[1];
				
				for (int index2=2; index2<TableInfo.length; index2++){
					RequestedFields +=  " , " + AliasTableName +"." + TableInfo[index2] ;
					if(index2==2 && createVCF){
						AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[index2] + " ) ";
					}
					else{
						if (!createVCF)
							AllFields +=  " , MAX( " + AliasTableName +"." + TableInfo[index2] + ") as " + TableInfo[1].split("\\.")[1] +"_"+ TableInfo[index2];
						else
							AllFields +=  " , " + AliasTableName +"." + TableInfo[index2];
					}
				}
										
				AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position ";
						if(VariantAnnotationTableNames!=null){
							AnnotationQuery += ", VCF.reference_bases, VCF.alternate_bases " ;
						}
				AnnotationQuery +=RequestedFields
					+ " FROM " + VCFQuery 
					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";

				AnnotationQuery += " WHERE "			
				//		x1 <= y2 && y1 <= x2
				+ "((("+ AliasTableName +".start_position <= VCF.end_position) AND ("+ AliasTableName +".end_position >= VCF.start_position)))";

		
				if(index+1 <  TranscriptAnnotations.length)
					AnnotationQuery +=  "), ";
				else
					AnnotationQuery += ") ";
			}
		
		}		
	
		String Query= "  SELECT VCF.reference_name as reference_name, VCF.start_position, VCF.end_position ";
				if(VariantAnnotationTableNames!=null){
					 Query+= ", VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases ";
				}
				
		Query += AllFields + " FROM ";
		
		Query += AnnotationQuery;
		if (!createVCF) {
			Query += " GROUP BY  reference_name, start_position, end_position "; 
			
			if(VariantAnnotationTableNames!=null){
				Query += ", reference_bases, alternate_bases";
			}
		}
		return Query;
	}
	
	

	
	public static String prepareGeneBasedAnnotationMinQueryConcatFieldsMinmVCF(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames, String TranscriptCanonicalizeRefNames,
			String TempMinTableName, boolean OnlyIntrogenic, boolean OutputASTable) {

		String[] TranscriptAnnotations=null;
		String[] VCFTables=null;
		String[] VCFCanonicalize=null; 
		String[] TranscriptCanonicalize=null;
	
		/////////////////////Transcripts//////////////
		if(TranscriptAnnotationTableNames!=null){
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
	
			if(!TranscriptCanonicalizeRefNames.isEmpty()){
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}

		//////////VCF Files/////////////
		/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
		if(!VCFTableNames.isEmpty()){
			VCFTables = VCFTableNames.split(","); 
		
			if(!VCFCanonicalizeRefNames.isEmpty()){
				VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
			else{
				System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}
		
				
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index < VCFTables.length; index++){
			if(VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, end_position, reference_bases, alternate_bases,  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, end_position, reference_bases, alternate_bases, ";

			if (index+1<VCFTables.length)
				VCFQuery += " FROM ["+ VCFTables[index]  +" OMIT RECORD IF EVERY(call.genotype <= 0) ), ";
			else
				VCFQuery += " FROM ["+ VCFTables[index]  +"] OMIT RECORD IF EVERY(call.genotype <= 0) ) ";
		}
		VCFQuery +=" ) as VCF ";

		 	 
		String AllFields=""; // Use to 
		String QueryCalcAll="";
		String AnnotationQuery="";
						
				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */
			
				//There is only one Annotation Dataset
				String [] TableInfo = TranscriptAnnotations[0].split(":");
										
				String RequestedFields="";
				String AliasTableName= "AN"; 
				
				//Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				//TableName = gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17
				String TableName = TableInfo[0]+":"+TableInfo[1];
				
				//AllFields = exonCount:exonStarts:exonEnds:score
				for (int index2=2; index2<TableInfo.length; index2++){
					RequestedFields +=  " , " + AliasTableName +"." + TableInfo[index2] ;			
					if(index2==2){
						AllFields += ", CONCAT(\"1: \","+ "CalcALL" +"." + AliasTableName +"." + TableInfo[index2] + " ) ";
					}
					else	
						AllFields +=  " , " + "CalcALL" +"." + AliasTableName + "." + TableInfo[index2];
				}

								
				QueryCalcAll += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases " + RequestedFields + ", AN.start_position, AN.end_position , "
						+ "CASE WHEN (ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) THEN ABS(VCF.start_position-AN.end_position) ELSE ABS(VCF.end_position-AN.start_position) end_position as distance "
						+ " FROM " + VCFQuery 
					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				QueryCalcAll += " ON " + AliasTableName + ".reference_name = VCF.reference_name"
						+ " ) as CalcALL ";

				
//				QueryCalcMin += " ( SELECT VCF.reference_name, VCF.start, VCF.END, MIN(CASE "
//						+ "WHEN (ABS(VCF.End-AN.Start) >= ABS(VCF.Start - AN.End)) THEN ABS(VCF.Start-AN.End) " + 
//						" ELSE ABS(VCF.End-AN.Start) END) as distance "
//						+ " FROM " + VCFQuery 
//						+ " JOIN [" + TableName +"] AS " + AliasTableName ;
//				QueryCalcMin += " ON " + AliasTableName + ".reference_name = VCF.reference_name "
//						+ "Group By VCF.reference_name, VCF.start, VCF.END "
//						+ " ) as CalcMin ";
//				
				AnnotationQuery += QueryCalcAll;
				AnnotationQuery += "join ";
//				AnnotationQuery += QueryCalcMin;		
				AnnotationQuery += "(SELECT     VCF_reference_name,     VCF_start,     VCF_END,      distance   FROM     [" + TempMinTableName +"] ) AS CalcMin "; 

				AnnotationQuery += " ON " + 
						"  CalcALL.VCF.reference_name = CalcMin.VCF_reference_name " + 
						"  AND CalcALL.VCF.start_position = CalcMin.VCF_start_position " + 
						"  AND CalcALL.VCF.end_position = CalcMin.VCF_end_position " + 
						"  AND CalcALL.distance = CalcMin.distance " ;

				
//				AnnotationQuery += "ON" + 
//						"  CalcALL.VCF.reference_name = CalcMin.VCF.reference_name " + 
//						"  AND CalcALL.VCF.start = CalcMin.VCF.start " + 
//						"  AND CalcALL.VCF.END = CalcMin.VCF.END " + 
//						"  AND CalcALL.distance = CalcMin.distance " ;
						
				
		String Query= "  SELECT "
				+ " CalcALL.VCF.reference_name as reference_name, CalcALL.VCF.start_position, CalcALL.VCF.end_position, "
				+ "CalcALL.VCF.reference_bases as reference_bases, CalcALL.VCF.alternate_bases as alternate_bases " + AllFields +  ", CalcALL.distance " + //, "
//				+ "  CASE " + 
//				"    WHEN " + 
//				"        ((CalcALL.AN.start <= CalcALL.VCF.END) AND (CalcALL.VCF.start <= CalcALL.AN.END)) " + 
//				"       THEN " + 
//				"          'Overlapped' " + 
//				"       ELSE " + 
//				"          'Closest' " + 
//				"  END AS Status " + 
				" FROM ";
		
		Query += AnnotationQuery;

		
//		if (OnlyIntrogenic) {
//			Query += "WHERE (( CalcALL.VCF.start> CalcALL.AN.End) OR (CalcALL.AN.Start> CalcALL.VCF.END))";
//		}
		
		return Query;
	}	


	public static String prepareGeneBasedAnnotationQueryConcatFieldsWithSampleNamesMin(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames, String TranscriptCanonicalizeRefNames,
			String SampleNames, boolean OnlyIntrogenic) {

		String[] TranscriptAnnotations=null;
		String[] VCFTables=null;
		String[] VCFCanonicalize=null; 
		String[] TranscriptCanonicalize=null;
	
		/////////////////////Transcripts//////////////
		if(TranscriptAnnotationTableNames!=null){
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
	
			if(!TranscriptCanonicalizeRefNames.isEmpty()){
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}

		//////////VCF Files/////////////
		/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
		if(!VCFTableNames.isEmpty()){
			VCFTables = VCFTableNames.split(","); 
		
			if(!VCFCanonicalizeRefNames.isEmpty()){
				VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
			else{
				System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}
		
				
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index < VCFTables.length; index++){
			if(VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, end_position, reference_bases, alternate_bases, call.call_set_name  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, end_position, reference_bases, alternate_bases, call.call_set_name ";

			if (index+1<VCFTables.length)
				VCFQuery += " FROM FLATTEN(["+ VCFTables[index]  +"], call.call_set_name) OMIT RECORD IF EVERY(call.genotype <= 0) ), ";
			else
				VCFQuery += " FROM FLATTEN(["+ VCFTables[index]  +"], call.call_set_name) OMIT RECORD IF EVERY(call.genotype <= 0) ) ";
		}
		VCFQuery +=" where call.call_set_name = \""+ SampleNames + "\") as VCF ";

		 	 
		String AllFields=""; // Use to 
		String QueryCalcAll="";
		String QueryCalcMin="";
		String AnnotationQuery="";
						
				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */
			
				//There is only one Annotation Dataset
				String [] TableInfo = TranscriptAnnotations[0].split(":");
										
				String RequestedFields="";
				String AliasTableName= "AN"; 
				
				//Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				//TableName = gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17
				String TableName = TableInfo[0]+":"+TableInfo[1];
				
				//AllFields = exonCount:exonStarts:exonEnds:score
				for (int index2=2; index2<TableInfo.length; index2++){
					RequestedFields +=  " , " + AliasTableName +"." + TableInfo[index2] ;			
					if(index2==2){
						AllFields += ", CONCAT(\"1: \","+ "CalcALL" +"." + AliasTableName +"." + TableInfo[index2] + " ) ";
					}
					else	
						AllFields +=  " , " + "CalcALL" +"." + AliasTableName + "." + TableInfo[index2];
				}

								
				QueryCalcAll += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases " + RequestedFields + ", AN.start_position, AN.end_position , ABS(VCF.start_position-AN.start_position) as distance "
					+ " FROM " + VCFQuery 
					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				QueryCalcAll += " ON " + AliasTableName + ".reference_name = VCF.reference_name) as CalcALL ";

				
				QueryCalcMin += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, MIN(ABS(VCF.start_position-AN.start_position)) as distance "
						+ " FROM " + VCFQuery 
						+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				QueryCalcMin += " ON " + AliasTableName + ".reference_name = VCF.reference_name "
						+ "Group By VCF.reference_name, VCF.start_position, VCF.end_position "
						+ " ) as CalcMin ";
				
				AnnotationQuery += QueryCalcAll;
				AnnotationQuery += "join ";
				AnnotationQuery += QueryCalcMin;		
				
				AnnotationQuery += "ON" + 
						"  CalcALL.VCF.reference_name = CalcMin.VCF.reference_name " + 
						"  AND CalcALL.VCF.start_position = CalcMin.VCF.start_position " + 
						"  AND CalcALL.VCF.end_position = CalcMin.VCF.end_position " + 
						"  AND CalcALL.distance = CalcMin.distance " ;
						
				
		String Query= "  SELECT "
				+ " CalcALL.VCF.reference_name as reference_name, CalcALL.VCF.start_position, CalcALL.VCF.end_position, "
				+ "CalcALL.VCF.reference_bases as reference_bases, CalcALL.VCF.alternate_bases as alternate_bases " + AllFields +  ", CalcALL.distance, "
				+ "  CASE " + 
				"    WHEN " + 
				"        ((CalcALL.AN.start_position <= CalcALL.VCF.end_position) AND (CalcALL.VCF.start_position <= CalcALL.AN.end_position)) " + 
				"       THEN " + 
				"          'Overlapped' " + 
				"       ELSE " + 
				"          'Closest' " + 
				"  end_position AS Status " + 
				" FROM ";
		
		Query += AnnotationQuery;
	
		if (OnlyIntrogenic) {
			Query += " WHERE (( CalcALL.VCF.start_position> CalcALL.AN.end_position) OR (CalcALL.AN.start_position> CalcALL.VCF.end_position))";
		}
		
		return Query;
	}
	
	public static String prepareGeneBasedAnnotationQueryConcatFieldsWithSampleNames(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			String TranscriptCanonicalizeRefNames, String SampleNames, int Threashold,
			boolean onlyIntrogenic) {

		String[] TranscriptAnnotations = null;
		String[] VCFTables = null;
		String[] VCFCanonicalize = null;
		String[] TranscriptCanonicalize = null;

		///////////////////// Transcripts//////////////
		if (TranscriptAnnotationTableNames != null) {
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(",");

			if (!TranscriptCanonicalizeRefNames.isEmpty()) {
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(",");
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException(
							"Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}

		////////// VCF Files/////////////
		/*
		 * Check if the VCF table contains any prefix for the reference fields (e.g.,
		 * chr => chr17 )
		 */
		if (!VCFTableNames.isEmpty()) {
			VCFTables = VCFTableNames.split(",");

			if (!VCFCanonicalizeRefNames.isEmpty()) {
				VCFCanonicalize = VCFCanonicalizeRefNames.split(",");
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException(
							"Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			} else {
				System.out.println(
						"#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}

		/* #######################Prepare VCF queries####################### */
		String VCFQuery = " ( SELECT * from ";
		for (int index = 0; index < VCFTables.length; index++) {
			if (VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '" + VCFCanonicalize[index]
						+ "', '') as reference_name, start_position, end_position, reference_bases, alternate_bases, call.call_set_name  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, end_position, reference_bases, alternate_bases, call.call_set_name ";

			if (index + 1 < VCFTables.length)
				VCFQuery += " FROM FLATTEN([" + VCFTables[index]
						+ "], call.call_set_name) OMIT RECORD IF EVERY(call.genotype <= 0) ), ";
			else
				VCFQuery += " FROM FLATTEN([" + VCFTables[index]
						+ "], call.call_set_name) OMIT RECORD IF EVERY(call.genotype <= 0) ) ";
		}
		VCFQuery += " where call.call_set_name = \"" + SampleNames + "\") as VCF ";

		String AnnotationQuery = "";

		/*
		 * Example:
		 * gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:
		 * exonStarts:exonEnds:score ProjectId: gbsc-gcp-project-cba DatasetId:
		 * PublicAnnotationSets TableId: hg19_refGene_chr17 Features:
		 * exonCount:exonStarts:exonEnds:score
		 */

		// There is only one Annotation Dataset
		String[] TableInfo = TranscriptAnnotations[0].split(":");
		String TopFields = "";
		String RequestedFields = "";
		String AliasTableName = "AN";

		// Example:
		// gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
		// TableName = gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17
		String TableName = TableInfo[0] + ":" + TableInfo[1];

		for (int index2 = 2; index2 < TableInfo.length; index2++) {
			RequestedFields += " , " + TableInfo[index2];
			if (index2 == 2) {
				TopFields += ", CONCAT(\"1: \"," + AliasTableName + "." + TableInfo[index2] + " ) ";
			} else
				TopFields += " , " + AliasTableName + "." + TableInfo[index2];
		}

		String Query = "  SELECT " + " VCF.reference_name as reference_name, VCF.start_position, VCF.end_position, "
				+ "VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases " + TopFields
				+ ", AN.start_position" + ", AN.end_position" + ", CASE"
				+ "      WHEN (ABS(VCF.end_position-AN.start_position)>=ABS(VCF.start_position - AN.end_position)) THEN ABS(VCF.start_position-AN.end_position)"
				+ "      ELSE ABS(VCF.end_position-AN.start_position)" + "    END AS distance" + ", CASE "
				+ "    WHEN ((VCF.start_position <= AN.end_position) AND (AN.start_position <= VCF.end_position)) THEN 'Overlapped' "
				+ "    ELSE 'Closest' " + "  END AS Status " + " FROM " + VCFQuery + " JOIN (" + "SELECT "
				+ "      reference_name " + "      , start_position  " + "      , end_position " + RequestedFields + " From " + "[" + TableName
				+ "]) AS " + AliasTableName + " ON " + AliasTableName + ".reference_name = VCF.reference_name ";

		Query = "Select * from (" + Query + ") WHERE distance < " + Threashold;

		if (onlyIntrogenic) {
			Query += " AND (( VCF.start_position> AN.end_position) OR (AN.start_position> VCF.end_position))";
		}

		Query += AnnotationQuery;

		return Query;
	}

	public static String prepareGeneBasedQueryConcatFields_mVCF(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames, String TranscriptCanonicalizeRefNames,
			 int Threashold, boolean onlyIntrogenic) {

		String[] TranscriptAnnotations=null;
		String[] VCFTables=null;
		String[] VCFCanonicalize=null; 
		String[] TranscriptCanonicalize=null;
	
		/////////////////////Transcripts//////////////
		if(TranscriptAnnotationTableNames!=null){
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
	
			if(!TranscriptCanonicalizeRefNames.isEmpty()){
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}

		//////////VCF Files/////////////
		/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
		if(!VCFTableNames.isEmpty()){
			VCFTables = VCFTableNames.split(","); 
		
			if(!VCFCanonicalizeRefNames.isEmpty()){
				VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
			else{
				System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}
		
				
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index< VCFTables.length; index++){
			if(VCFCanonicalize!=null)
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, end_position, reference_bases, alternate_bases, call.call_set_name, quality  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, end_position, reference_bases, alternate_bases, call.call_set_name, quality ";

			if (index+1<VCFTables.length)
				VCFQuery += " FROM ["+ VCFTables[index]  +"] OMIT RECORD IF EVERY(call.genotype <= 0) ), ";
			else
				VCFQuery += " FROM ["+ VCFTables[index]  +"] OMIT RECORD IF EVERY(call.genotype <= 0) ) ";
		}
		VCFQuery +=" ) as VCF ";
		 	 
						
				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */
			
				//There is only one Annotation Dataset
				String [] TableInfo = TranscriptAnnotations[0].split(":");
				String TopFields="";						
				String RequestedFields="";
				String AliasTableName= "AN"; 
				
				//Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				//TableName = gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17
				String TableName = TableInfo[0]+":"+TableInfo[1];

				for (int index2=2; index2<TableInfo.length; index2++){
					RequestedFields +=  " , " +  TableInfo[index2] ;		
					if(index2==2){
						TopFields += ", CONCAT(\"1: \","+  AliasTableName +"." + TableInfo[index2] + " ) ";
					}
					else	
						TopFields +=  " , " + AliasTableName + "." + TableInfo[index2];
				}

									
		String Query= "  SELECT "
				+ " VCF.reference_name as reference_name, VCF.start_position, VCF.end_position, "
				+ "VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases " 
				+ TopFields
				+ ", AN.start_position"
				+ ", AN.end_position"
				+ ", CASE" 
				+ "      WHEN (ABS(VCF.end_position-AN.start_position)>=ABS(VCF.start_position - AN.end_position)) THEN ABS(VCF.start_position-AN.end_position)"  
				+ "      ELSE ABS(VCF.end_position-AN.start_position)" 
				+ "    END AS distance"
				+ ", CASE "
				+ "    WHEN ((VCF.start_position <= AN.end_position) AND (AN.start_position <= VCF.end_position)) THEN 'Overlapped' "
				+ "    ELSE 'Closest' "  
				+ "  END AS Status "
				+ " FROM " + VCFQuery
				+ " JOIN ("
				+ "SELECT " 
				+ "      reference_name "  
				+ "      , start_position  "  
				+ "      , end_position "  
				+ RequestedFields
				+ " From "
				+ "[" + TableName +"]) AS " + AliasTableName
				+ " ON " + AliasTableName + ".reference_name = VCF.reference_name ";
		if (onlyIntrogenic) {
			Query += " WHERE (( VCF.start_position> AN.end_position) OR (AN.start_position> VCF.end_position))";
		}

		Query = "Select * from (" + Query + ") WHERE distance < " + Threashold;
		
		
		return Query;
	}

	
	public static String prepareGeneBasedQueryConcatFields_mVCF_Range_Min_StandardSQL_RefGene(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			String TranscriptCanonicalizeRefNames, int Threashold, boolean onlyIntrogenic, boolean OutputASTable,
			boolean MIN, String searchRegions) {

		String[] TranscriptAnnotations = null;
		String[] VCFTables = null;
		String[] VCFCanonicalize = null;
		String[] TranscriptCanonicalize = null;

		///////////////////// Transcripts//////////////
		if (TranscriptAnnotationTableNames != null) {
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(",");

			if (!TranscriptCanonicalizeRefNames.isEmpty()) {
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(",");
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException(
							"Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}

		
		String WHERE_VCF=" ";
		
		if (!searchRegions.isEmpty()) {
			String[] temp = searchRegions.split(",");
			WHERE_VCF += " AND ";
			for (int index=0; index <temp.length; index++) {
				String [] RS = temp[index].split(":");
				if(index+1<temp.length) {
					WHERE_VCF += " (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start_position > " + RS[1] + " AND start_position < " + RS[2] + ") OR " ;
				}
				else {
					WHERE_VCF += " (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start_position > " + RS[1] + " AND start_position < " + RS[2] + ") " ;
				}
			}
			
		}

		
		////////// VCF Files/////////////
		/*
		 * Check if the VCF table contains any prefix for the reference fields (e.g.,
		 * chr => chr17 )
		 */
		if (!VCFTableNames.isEmpty()) {
			VCFTables = VCFTableNames.split(",");

			if (!VCFCanonicalizeRefNames.isEmpty()) {
				VCFCanonicalize = VCFCanonicalizeRefNames.split(",");
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException(
							"Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			} else {
				System.out.println(
						"#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}

		/* #######################Prepare VCF queries####################### */
		String VCFQuery = "";
		for (int index = 0; index < VCFTables.length; index++) {
			if (VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '" + VCFCanonicalize[index]
						+ "', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases ";

			if (index + 1 < VCFTables.length)
				VCFQuery += " FROM `" + VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]
						+ "`   WHERE EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt WHERE alt NOT IN (\"<NON_REF>\", \"<*>\") ";
			else
				VCFQuery += " FROM `" + VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]
						+ "` WHERE EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt WHERE alt NOT IN (\"<NON_REF>\", \"<*>\")  ";
		}
		
		if (!searchRegions.isEmpty()) {
			VCFQuery += WHERE_VCF;
		}
		
		VCFQuery += " )) as VCF ";

		String AllFields = ""; // Use to
		String RequestedFields = "";

		/*
		 * Example:
		 * gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:
		 * exonStarts:exonEnds:score ProjectId: gbsc-gcp-project-cba DatasetId:
		 * PublicAnnotationSets TableId: hg19_refGene_chr17 Features:
		 * exonCount:exonStarts:exonEnds:score
		 */

		// There is only one Annotation Dataset
		String[] TableInfo = TranscriptAnnotations[0].split(":");

		// Example:
		// gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
		// TableName = gbsc-gcp-project-cba.PublicAnnotationSets.hg19_refGene_chr17
		String TableName = TableInfo[0] + "." + TableInfo[1];

		// AllFields = exonCount:exonStarts:exonEnds:score
		for (int index2 = 2; index2 < TableInfo.length; index2++) {
			RequestedFields += " , " + TableInfo[index2];

			if (index2 == 2) {
				if (!OutputASTable)
					AllFields += " CONCAT(\"1: \"," + "AN" + "." + TableInfo[index2] + " ) ";
				else
					AllFields += "AN" + "." + TableInfo[index2];
			} else
				AllFields += " , " + "AN" + "." + TableInfo[index2];
		}

		String Query = "";
		if (MIN) {
//			Query = "SELECT " + "    VCF.reference_name as reference_name," + "    VCF.start as start," + "    VCF.END as `end`,"
//					+ "    VCF.reference_bases as reference_bases,"
//					+ "    ARRAY_TO_STRING(VCF.alternate_bases, ',') as alternate_bases," + "	   ARRAY_AGG(("
//					+ AllFields + ")" + "    ORDER BY "
//					+ "     (CASE         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.END)) AND         "
//					+ " (ABS(VCF.start-AN.Start) >= ABS(VCF.Start - AN.END)) AND         "
//					+ " (ABS(VCF.end-AN.end) >= ABS(VCF.Start - AN.END)))         "
//					+ "  THEN ABS(VCF.Start-AN.END)         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.start)) AND         "
//					+ "(ABS(VCF.end-AN.end) >= ABS(VCF.Start - AN.start)))          "
//					+ "THEN ABS(VCF.Start-AN.Start)         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.end - AN.end)))          "
//					+ "THEN ABS(VCF.END-AN.END)         ELSE ABS(VCF.END-AN.Start) END) " + "    LIMIT "
//					+ "      1 )[SAFE_OFFSET(0)] names " + ", CASE " + "		 WHEN "
//					+ "        ((AN.start >  VCF.END) OR ( VCF.start > AN.END)) " + "        THEN "
//					+ "        'Closest' " + "        ELSE " + "        'Overlapped'" + "       END AS Status "
					
			Query = 	"SELECT" + 
					"  reference_name," + 
					"  start_position," + 
					"  `end_position`," + 
					"  reference_bases," + 
					"  alternate_bases," + 
					"  ARRAY_AGG (STRUCT(names," + 
					"     REPLACE(REPLACE(REPLACE(status, '1', ''), '2', ''), '3', ''))" + 
					"  ORDER BY" + 
					"    Status ASC" + 
					"  LIMIT" + 
					"    1)[" + 
					"OFFSET" + 
					"  (0)].*" + 
					"FROM (" + 
					"  SELECT" + 
					"    VCF.reference_name AS reference_name," + 
					"    VCF.start_position," + 
					"    VCF.end_position," + 
					"    VCF.reference_bases AS reference_bases," + 
					"    ARRAY_TO_STRING(VCF.alternate_bases, ',') AS alternate_bases," + 
					"    ARRAY_AGG ((CONCAT(AN.name, \";\" , AN.name2, \";\", \"dist:\", CAST((CASE" + 
					"          WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) AND "
					+ "(ABS(VCF.start_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) AND "
					+ "(ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.end_position))) THEN ABS(VCF.start_position-AN.end_position)" + 
					"          WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.start_position))" + 
					"          AND (ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.start_position))) THEN ABS(VCF.start_position-AN.start_position)" + 
					"          WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.end_position - AN.end_position))) THEN ABS(VCF.end_position-AN.end_position)" + 
					"          ELSE ABS(VCF.end_position-AN.start_position) END) as STRING) ))" + 
					"    ORDER BY" + 
					"      (CASE" + 
					"          WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) "
					+ "AND (ABS(VCF.start_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) AND "
					+ "(ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.end_position))) THEN ABS(VCF.start_position-AN.end_position)" + 
					"          WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.start_position))" + 
					"          AND (ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.start_position))) THEN ABS(VCF.start_position-AN.start_position)" + 
					"          WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.end_position - AN.end_position))) THEN ABS(VCF.end_position-AN.end_position)" + 
					"          ELSE ABS(VCF.end_position-AN.start_position) END)" + 
					"    LIMIT\n" + 
					"      1 )[SAFE_OFFSET(0)] names," + 
					"    CASE" + 
					"      WHEN ((AN.start_position > VCF.end_position) OR ( VCF.start_position > AN.end_position)) THEN '3Intergenic'" + 
					"      WHEN ((AN.start_positionPoint <= VCF.end_position)" + 
					"      AND (VCF.start_position <= AN.EndPoint)) THEN '1Exonic'" + 
					"      ELSE '2Intronic'" + 
					"    END AS Status "	
					+ " FROM " + VCFQuery + " JOIN (\n" + 
							"    SELECT " + 
							"      reference_name," + 
							"      start_position, " + 
							"      `end_position`, " + 
						//TODO: Dynamic fields 
							"      name, " + 
							"      name2, " +
						//TODO: Dynamic exon starts and ends
							"      CAST(StartPoint AS INT64) StartPoint, " + 
							"      CAST(EndPoint AS INT64) EndPoint " + 
							"    FROM " +
							"`" + TableName + "` t, " +
							"      UNNEST(SPLIT( exonStarts )) StartPoint " + 
							"    WITH " + 
							"    OFFSET " + 
							"      pos1 " + 
							"    JOIN " + 
							"      UNNEST(SPLIT( exonEnds )) EndPoint " + 
							"    WITH " + 
							"    OFFSET " + 
							"      pos2 " + 
							"    ON " + 
							"      pos1 = pos2 " + 
							"    WHERE " + 
							"      EndPoint<>\"\") AS AN " + 
							"   ON VCF.reference_name = AN.reference_name ";
			if (onlyIntrogenic)
				Query += " WHERE (VCF.start_position>AN.end_position) OR (AN.start_position> VCF.end_position) ";
			Query += "   GROUP BY" + 
					"    VCF.reference_name," + 
					"    VCF.start_position," + 
					"    VCF.end_position," + 
					"    VCF.reference_bases," + 
					"    alternate_bases," + 
					"    Status )" + 
					"GROUP BY" + 
					"  reference_name," + 
					"  start_position," + 
					"  `end_position`," + 
					"  reference_bases," + 
					"  alternate_bases";

			// :TODO M and MT case
		} else {
			Query = "  SELECT " + " VCF.reference_name as reference_name, VCF.start_position, VCF.end_position, "
					+ "VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, " + AllFields
					+ ", AN_start" + ", AN_end" + ", CASE"
					+ "      WHEN (ABS(VCF.end_position-AN_Start)>=ABS(VCF.start_position - AN_End)) THEN ABS(VCF.start_position-AN_End)"
					+ "      ELSE ABS(VCF.end_position-AN_Start)" + "    END AS distance" + ", CASE "
					+ "    WHEN ((VCF.start_position <= AN_End) AND (AN_Start <= VCF.end_position)) THEN 'Overlapped' "
					+ "    ELSE 'Closest' " + "  END AS Status " + " FROM " + VCFQuery + " JOIN (" + "SELECT "
					+ "      reference_name " + "      , start_position as AN_Start  " + "      , `end_position` as AN_END " + RequestedFields
					+ " From " + "`" + TableName + "`) AS AN " + " ON AN.reference_name = VCF.reference_name ";
			// NOTE: It finds all genes within the range irrespective of whether a variant
			// overlapped w/ any gene or not.
			/*
			 * if (onlyIntrogenic) { Query +=
			 * " WHERE (( VCF.start> AN.End) OR (AN.Start> VCF.END))"; }
			 */

			Query = "Select * from (" + Query + ") WHERE distance < " + Threashold;

		}

		return Query;
	}

	public static String prepareGeneBasedQueryConcatFields_mVCF_Range_Min_StandardSQL(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			String TranscriptCanonicalizeRefNames, int Threashold, boolean onlyIntrogenic, boolean createVCF,
			boolean MIN, String searchRegions, boolean GoogleVCF) {

		String[] TranscriptAnnotations = null;
		String[] VCFTables = null;
		String[] VCFCanonicalize = null;
		String[] TranscriptCanonicalize = null;

		///////////////////// Transcripts//////////////
		if (TranscriptAnnotationTableNames != null) {
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(",");

			if (!TranscriptCanonicalizeRefNames.isEmpty()) {
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(",");
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException(
							"Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}

		
		String WHERE_VCF=" ";
		
		if (!searchRegions.isEmpty()) {
			String[] temp = searchRegions.split(",");
			for (int index=0; index <temp.length; index++) {
				String [] RS = temp[index].split(":");
				if(index+1<temp.length) {
					WHERE_VCF += " AND (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start_position > " + RS[1] + " AND start_position < " + RS[2] + ") OR " ;
				}
				else {
					WHERE_VCF += " AND (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start_position > " + RS[1] + " AND start_position < " + RS[2] + ") " ;
				}
			}
			
		}

		
		////////// VCF Files/////////////
		/*
		 * Check if the VCF table contains any prefix for the reference fields (e.g.,
		 * chr => chr17 )
		 */
		if (!VCFTableNames.isEmpty()) {
			VCFTables = VCFTableNames.split(",");

			if (!VCFCanonicalizeRefNames.isEmpty()) {
				VCFCanonicalize = VCFCanonicalizeRefNames.split(",");
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException(
							"Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			} else {
				System.out.println(
						"#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}

		/* #######################Prepare VCF queries####################### */
		String VCFQuery = "";
		for (int index = 0; index < VCFTables.length; index++) {
			if (VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '" + VCFCanonicalize[index]
						+ "', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases ";

			if (index + 1 < VCFTables.length) {
				VCFQuery += " FROM `" + VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]
						+ "`   WHERE EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt ";
				if(GoogleVCF)
					VCFQuery += " WHERE alt NOT IN (\"<NON_REF>\", \"<*>\") ";
			}
			else {
				VCFQuery += " FROM `" + VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]
						+ "` WHERE EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt ";
				if(GoogleVCF)
					VCFQuery += " WHERE alt NOT IN (\"<NON_REF>\", \"<*>\") ";
			}
		}
		
		if (!searchRegions.isEmpty()) {
			VCFQuery += WHERE_VCF;
		}
		
		VCFQuery += " )) as VCF ";

		String AllFields = ""; // Use to
		String RequestedFields = "";

		/*
		 * Example:
		 * gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:
		 * exonStarts:exonEnds:score ProjectId: gbsc-gcp-project-cba DatasetId:
		 * PublicAnnotationSets TableId: hg19_refGene_chr17 Features:
		 * exonCount:exonStarts:exonEnds:score
		 */

		// There is only one Annotation Dataset
		String[] TableInfo = TranscriptAnnotations[0].split(":");

		// Example:
		// gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
		// TableName = gbsc-gcp-project-cba.PublicAnnotationSets.hg19_refGene_chr17
		String TableName = TableInfo[0] + "." + TableInfo[1];

		// AllFields = exonCount:exonStarts:exonEnds:score
		for (int index2 = 2; index2 < TableInfo.length; index2++) {
			RequestedFields += " , " + TableInfo[index2];

			if (index2 == 2) {
				if (createVCF)
					AllFields += " CONCAT(\"1: \"," + "AN" + "." + TableInfo[index2] + " ) ";
				else
					AllFields += "AN" + "." + TableInfo[index2];
			} else
				AllFields += " , " + "AN" + "." + TableInfo[index2];
		}

		String Query = "";
		if (MIN) {
			Query = "SELECT " + "    VCF.reference_name as reference_name," + "    VCF.start_position," + "    VCF.end_position ,"
					+ "    VCF.reference_bases as reference_bases,"
					+ "    ARRAY_TO_STRING(VCF.alternate_bases, ',') as alternate_bases," + "	   ARRAY_AGG(("
					+ AllFields + ")" + "    ORDER BY "
					+ "     (CASE         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) AND         "
					+ " (ABS(VCF.start_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) AND         "
					+ " (ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.end_position)))         "
					+ "  THEN ABS(VCF.start_position-AN.end_position)         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.start_position)) AND         "
					+ "(ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.start_position)))          "
					+ "THEN ABS(VCF.start_position-AN.start_position)         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.end_position - AN.end_position)))          "
					+ "THEN ABS(VCF.end_position-AN.end_position)         ELSE ABS(VCF.end_position-AN.start_position) END) " + "    LIMIT "
					+ "      1 )[SAFE_OFFSET(0)] names " + ", CASE " + "		 WHEN "
					+ "        ((AN.start_position >  VCF.end_position) OR ( VCF.start_position > AN.end_position)) " + "        THEN "
					+ "        'Closest' " + "        ELSE " + "        'Overlapped'" + "       END AS Status "
					+ " FROM " + VCFQuery + " JOIN `" + TableName + "` AS AN " + "  ON VCF.reference_name = AN.reference_name ";
			if (onlyIntrogenic)
				Query += " WHERE (VCF.start_position>AN.end_position) OR (AN.start_position> VCF.end_position) ";
			Query += " GROUP BY VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, alternate_bases, Status";

			// :TODO M and MT case
		} else {
			Query = "  SELECT " + " VCF.reference_name as reference_name, VCF.start_position as start_position, VCF.end_position, "
					+ "VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, " + AllFields
					+ ", AN_start_position" + ", AN_end" + ", CASE"
					+ "      WHEN (ABS(VCF.end_position-AN_start_position)>=ABS(VCF.start_position - AN_End)) THEN ABS(VCF.start_position-AN_End)"
					+ "      ELSE ABS(VCF.end_position-AN_Start)" + "    END AS distance" + ", CASE "
					+ "    WHEN ((VCF.start_position <= AN_End) AND (AN_Start <= VCF.end_position)) THEN 'Overlapped' "
					+ "    ELSE 'Closest' " + "  END AS Status " + " FROM " + VCFQuery + " JOIN (" + "SELECT "
					+ "      reference_name " + "      , start_position as AN_Start  " + "      , `end_position` as AN_END " + RequestedFields
					+ " From " + "`" + TableName + "`) AS AN " + " ON AN.reference_name = VCF.reference_name ";
			// NOTE: It finds all genes within the range irrespective of whether a variant
			// overlapped w/ any gene or not.
			/*
			 * if (onlyIntrogenic) { Query +=
			 * " WHERE (( VCF.start> AN.End) OR (AN.Start> VCF.END))"; }
			 */

			Query = "Select * from (" + Query + ") WHERE distance < " + Threashold;

		}

		return Query;
	}


	public static String prepareGeneBasedAnnotationMinTablemVCF(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames, String TranscriptCanonicalizeRefNames, boolean onlyIntrogenic) {

		String[] TranscriptAnnotations=null;
		String[] VCFTables=null;
		String[] VCFCanonicalize=null; 
		String[] TranscriptCanonicalize=null;
	
		/////////////////////Transcripts//////////////
		if(TranscriptAnnotationTableNames!=null){
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
	
			if(!TranscriptCanonicalizeRefNames.isEmpty()){
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}

		//////////VCF Files/////////////
		/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
		if(!VCFTableNames.isEmpty()){
			VCFTables = VCFTableNames.split(","); 
		
			if(!VCFCanonicalizeRefNames.isEmpty()){
				VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
			else{
				System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}
		
				
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index< VCFTables.length; index++){
			if(VCFCanonicalize!=null)
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, end_position, reference_bases, alternate_bases, call.call_set_name, quality  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, end_position, reference_bases, alternate_bases, call.call_set_name, quality ";

			if (index+1<VCFTables.length)
				VCFQuery += " FROM ["+ VCFTables[index]  +"] OMIT RECORD IF EVERY(call.genotype <= 0) ), ";
			else
				VCFQuery += " FROM ["+ VCFTables[index]  +"] OMIT RECORD IF EVERY(call.genotype <= 0) ) ";
		}
		VCFQuery +=" ) as VCF ";
		 	 
						
				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */
			
				//There is only one Annotation Dataset
				String [] TableInfo = TranscriptAnnotations[0].split(":");
				
				//Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				//TableName = gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17
				String TableName = TableInfo[0]+":"+TableInfo[1];
					
				
				String Query = "SELECT   VCF.reference_name,   VCF.start_position,   VCF.end_position,   MIN(CASE     WHEN (ABS(VCF.end_position-AN.start_position)>=ABS(VCF.start_position - AN.end_position)) "
							+ " THEN ABS(VCF.start_position - AN.end_position)     ELSE ABS(VCF.end_position-AN.start_position)   END) AS distance FROM "+ VCFQuery 
							+ " JOIN [" + TableName +"] AS AN ON  AN.reference_name = VCF.reference_name "
							+ " WHERE (( VCF.start_position> AN.end_position)  OR (AN.start_position> VCF.end_position)) "
							+ " Group By VCF.reference_name,   VCF.start_position,   VCF.end_position ";		

				return Query;

	}
	
	
	public static String prepareGeneBasedAnnotationMinQueryConcatFields_Min_mVCF_SQLStandard(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames, String TranscriptCanonicalizeRefNames, 
			boolean OnlyIntrogenic, boolean OutputASTable) {

		String[] TranscriptAnnotations=null;
		String[] VCFTables=null;
		String[] VCFCanonicalize=null; 
		String[] TranscriptCanonicalize=null;
	
		/////////////////////Transcripts//////////////
		if(TranscriptAnnotationTableNames!=null){
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
	
			if(!TranscriptCanonicalizeRefNames.isEmpty()){
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}

		//////////VCF Files/////////////
		/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
		if(!VCFTableNames.isEmpty()){
			VCFTables = VCFTableNames.split(","); 
		
			if(!VCFCanonicalizeRefNames.isEmpty()){
				VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
			else{
				LOG.warning("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}
		
				
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery="";
		for (int index=0; index < VCFTables.length; index++){
			if(VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases ";

			if (index+1<VCFTables.length)
				VCFQuery += " FROM `"+ VCFTables[index].split(":")[0] + "."+  VCFTables[index].split(":")[1] +"`   WHERE EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt WHERE alt NOT IN (\"<NON_REF>\", \"<*>\")) ";
			else
				VCFQuery += " FROM `"+ VCFTables[index].split(":")[0] + "."+  VCFTables[index].split(":")[1]   +"` WHERE EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt WHERE alt NOT IN (\"<NON_REF>\", \"<*>\")) ";
		}
		VCFQuery +=" ) as VCF ";

		 	 
		String AllFields=""; // Use to 
						
				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */
			
				//There is only one Annotation Dataset
				String [] TableInfo = TranscriptAnnotations[0].split(":");
														
				//Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				//TableName = gbsc-gcp-project-cba.PublicAnnotationSets.hg19_refGene_chr17
				String TableName = TableInfo[0]+"."+TableInfo[1];
				
				//AllFields = exonCount:exonStarts:exonEnds:score
				for (int index2=2; index2<TableInfo.length; index2++){
					if(index2==2){
						if(!OutputASTable)
							AllFields += " CONCAT(\"1: \","+ "AN" +"." + TableInfo[index2] + " ) ";
						else
							AllFields +=  "AN" +"." + TableInfo[index2];
					}
					else	
						AllFields +=  " , " + "AN" +"." + TableInfo[index2];
				}

				String Query = "SELECT " 
						+ "    VCF.reference_name as reference_name," 
						+ "    VCF.start_position,"  
						+ "    VCF.end_position," 
						+ "    VCF.reference_bases as reference_bases," 
						+ "    ARRAY_TO_STRING(VCF.alternate_bases, ',') as alternate_bases,"
						+ "	   ARRAY_AGG((" + AllFields +")"
						+ "    ORDER BY " 
//						"      (CASE " + 
//						"          WHEN (ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.END)) THEN ABS(VCF.Start-AN.END) " + 
//						"          ELSE ABS(VCF.END-AN.Start) END) "
						+ "     (CASE         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) AND         "
						+ " (ABS(VCF.start_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) AND         "
						+ " (ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.end_position)))         "
						+ "  THEN ABS(VCF.start_position-AN.end_position)         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.start_position)) AND         "
						+ "(ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.start_position)))          "
						+ "THEN ABS(VCF.start_position-AN.start_position)         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.end_position - AN.end_position)))          "
						+ "THEN ABS(VCF.end_position-AN.end_position)         ELSE ABS(VCF.end_position-AN.start_position) END) "
						+ "    LIMIT " 
						+ "      1 )[SAFE_OFFSET(0)] names "
						+", CASE " + 
						"		 WHEN " + 
						"        ((AN.start_position >  VCF.end_position) OR ( VCF.start_position > AN.end_position)) " + 
						"        THEN " + 
						"        'Closest' " + 
						"        ELSE " + 
						"        'Overlapped'" + 
						"       END AS Status "
						+ " FROM " + VCFQuery 
						+ " JOIN `"+ TableName +"` AS AN "
						+ "  ON VCF.reference_name = AN.reference_name ";
						if (OnlyIntrogenic)
							Query += " WHERE (VCF.start_position>AN.end_position) OR (AN.start_position> VCF.end_position) ";
						Query += " GROUP BY VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, alternate_bases, Status";
						
					//:TODO M and MT case				
				
				
				
						
		return Query;
	}



	public static String prepareGeneBasedAnnotationMinQueryConcatFields_Min_SQLStandard(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames, String TranscriptCanonicalizeRefNames, String SampleId, 
			boolean OnlyIntrogenic, boolean OutputASTable) {

		String[] TranscriptAnnotations=null;
		String[] VCFTables=null;
		String[] VCFCanonicalize=null; 
		String[] TranscriptCanonicalize=null;
	
		/////////////////////Transcripts//////////////
		if(TranscriptAnnotationTableNames!=null){
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
	
			if(!TranscriptCanonicalizeRefNames.isEmpty()){
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}

		//////////VCF Files/////////////
		/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
		if(!VCFTableNames.isEmpty()){
			VCFTables = VCFTableNames.split(","); 
		
			if(!VCFCanonicalizeRefNames.isEmpty()){
				VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
			else{
				System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}
		
		
				
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery="";
		for (int index=0; index < VCFTables.length; index++){
			if(VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases ";

			if (index+1<VCFTables.length)
				VCFQuery += " FROM `"+ VCFTables[index].split(":")[0] + "."+  VCFTables[index].split(":")[1] +"` v, v.call  WHERE call.call_set_name= \""+ SampleId +"\" AND EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt WHERE alt NOT IN (\"<NON_REF>\", \"<*>\")) ";
			else
				VCFQuery += " FROM `"+ VCFTables[index].split(":")[0] + "."+  VCFTables[index].split(":")[1]   +"` v, v.call  WHERE call.call_set_name=\""+ SampleId + "\" AND EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt WHERE alt NOT IN (\"<NON_REF>\", \"<*>\")) ";
		}
		VCFQuery +=" ) as VCF ";

		 	 
		String AllFields=""; // Use to 
						
				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */
			
				//There is only one Annotation Dataset
				String [] TableInfo = TranscriptAnnotations[0].split(":");
														
				//Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				//TableName = gbsc-gcp-project-cba.PublicAnnotationSets.hg19_refGene_chr17
				String TableName = TableInfo[0]+"."+TableInfo[1];
				
				//AllFields = exonCount:exonStarts:exonEnds:score
				for (int index2=2; index2<TableInfo.length; index2++){
					if(index2==2){
						if(!OutputASTable)
							AllFields += " CONCAT(\"1: \","+ "AN" +"." + TableInfo[index2] + " ) ";
						else
							AllFields +=  "AN" +"." + TableInfo[index2];
					}
					else	
						AllFields +=  " , " + "AN" +"." + TableInfo[index2];
				}

		String Query = "SELECT " 
				+ "    VCF.reference_name as reference_name," 
				+ "    VCF.start_position,"  
				+ "    VCF.end_position ," 
				+ "    VCF.reference_bases as reference_bases," 
				+ "    ARRAY_TO_STRING(VCF.alternate_bases, ',') as alternate_bases,"
				+ "	   ARRAY_AGG((" + AllFields +")"
				+ "    ORDER BY " 
//				"      (CASE " + 
//				"          WHEN (ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.END)) THEN ABS(VCF.Start-AN.END) " + 
//				"          ELSE ABS(VCF.END-AN.Start) END) "
				+ "     (CASE         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) AND         "
				+ " (ABS(VCF.start_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) AND         "
				+ " (ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.end_position)))         "
				+ "  THEN ABS(VCF.start_position-AN.end_position)         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.start_position)) AND         "
				+ "(ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.start_position)))          "
				+ "THEN ABS(VCF.start_position-AN.start_position)         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.end_position - AN.end_position)))          "
				+ "THEN ABS(VCF.end_position-AN.end_position)         ELSE ABS(VCF.end_position-AN.start_position) END) "
				+ "    LIMIT " 
				+ "      1 )[SAFE_OFFSET(0)] names "
				+", CASE " + 
				"		 WHEN " + 
				"        ((AN.start_position >  VCF.end_position) OR ( VCF.start_position > AN.end_position)) " + 
				"        THEN " + 
				"        'Closest' " + 
				"        ELSE " + 
				"        'Overlapped'" + 
				"       END AS Status "
				+ " FROM " + VCFQuery 
				+ " JOIN `"+ TableName +"` AS AN "
				+ "  ON VCF.reference_name = AN.reference_name ";
				if (OnlyIntrogenic)
					Query += " WHERE (VCF.start_position>AN.end_position) OR (AN.start_position> VCF.end_position) ";
				Query += " GROUP BY VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, "
						+ "alternate_bases, Status";
				
			//:TODO M and MT case
			
		
		return Query;
	}

	public static String prepareGeneBasedQueryConcatFields_Range_Min_StandardSQL(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			String TranscriptCanonicalizeRefNames, String SampleId, int Threashold, boolean onlyIntrogenic,
			boolean createVCF, boolean MIN, String searchRegions, boolean GoogleVCF) {

		String[] TranscriptAnnotations = null;
		String[] VCFTables = null;
		String[] VCFCanonicalize = null;
		String[] TranscriptCanonicalize = null;

		///////////////////// Transcripts//////////////
		if (TranscriptAnnotationTableNames != null) {
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(",");

			if (!TranscriptCanonicalizeRefNames.isEmpty()) {
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(",");
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException(
							"Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}

		
		String WHERE_VCF=" ";
		
		if (!searchRegions.isEmpty()) {
			String[] temp = searchRegions.split(",");
			for (int index=0; index <temp.length; index++) {
				String [] RS = temp[index].split(":");
				if(index+1<temp.length) {
					WHERE_VCF += " AND (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start_position > " + RS[1] + " AND start_position < " + RS[2] + ") OR " ;
				}
				else {
					WHERE_VCF += " AND (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start_position > " + RS[1] + " AND start_position < " + RS[2] + ") " ;
				}
			}
			
		}

		
		////////// VCF Files/////////////
		/*
		 * Check if the VCF table contains any prefix for the reference fields (e.g.,
		 * chr => chr17 )
		 */
		if (!VCFTableNames.isEmpty()) {
			VCFTables = VCFTableNames.split(",");

			if (!VCFCanonicalizeRefNames.isEmpty()) {
				VCFCanonicalize = VCFCanonicalizeRefNames.split(",");
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException(
							"Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			} else {
				System.out.println(
						"#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}

		/* #######################Prepare VCF queries####################### */
		String VCFQuery = "";
		for (int index = 0; index < VCFTables.length; index++) {
			if (VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '" + VCFCanonicalize[index]
						+ "', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases ";

			if (index + 1 < VCFTables.length) {
				VCFQuery += " FROM `" + VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]
						+ "` v, v.call  WHERE call.call_set_name= \"" + SampleId
						+ "\" AND EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt ";
				if(GoogleVCF)		
					VCFQuery += " WHERE alt NOT IN (\"<NON_REF>\", \"<*>\") ";
			}
			else {
				VCFQuery += " FROM `" + VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]
						+ "` v, v.call  WHERE call.call_set_name=\"" + SampleId
						+ "\" AND EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt ";
				if(GoogleVCF)		
					VCFQuery += " WHERE alt NOT IN (\"<NON_REF>\", \"<*>\") ";
				}
		}
		
		if (!searchRegions.isEmpty()) {
			VCFQuery += WHERE_VCF;
		}
		
		VCFQuery += " )) as VCF ";

		String AllFields = ""; // Use to
		String RequestedFields = "";

		/*
		 * Example:
		 * gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:
		 * exonStarts:exonEnds:score ProjectId: gbsc-gcp-project-cba DatasetId:
		 * PublicAnnotationSets TableId: hg19_refGene_chr17 Features:
		 * exonCount:exonStarts:exonEnds:score
		 */

		// There is only one Annotation Dataset
		String[] TableInfo = TranscriptAnnotations[0].split(":");

		// Example:
		// gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
		// TableName = gbsc-gcp-project-cba.PublicAnnotationSets.hg19_refGene_chr17
		String TableName = TableInfo[0] + "." + TableInfo[1];

		// AllFields = exonCount:exonStarts:exonEnds:score
		for (int index2 = 2; index2 < TableInfo.length; index2++) {
			RequestedFields += " , " + TableInfo[index2];

			if (index2 == 2) {
				if (createVCF)
					AllFields += " CONCAT(\"1: \"," + "AN" + "." + TableInfo[index2] + " ) ";
				else
					AllFields += "AN" + "." + TableInfo[index2];
			} else
				AllFields += " , " + "AN" + "." + TableInfo[index2];
		}

		String Query = "";
		if (MIN) {
			Query = "SELECT " + "    VCF.reference_name as reference_name," + "    VCF.start_position," + "    VCF.end_position,"
					+ "    VCF.reference_bases as reference_bases,"
					+ "    ARRAY_TO_STRING(VCF.alternate_bases, ',') as alternate_bases," + "	   ARRAY_AGG(("
					+ AllFields + ")" + "    ORDER BY "
					+ "     (CASE         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) AND         "
					+ " (ABS(VCF.start_position-AN.start_position) >= ABS(VCF.start_position - AN.end_position)) AND         "
					+ " (ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.end_position)))         "
					+ "  THEN ABS(VCF.start_position-AN.end_position)         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.start_position - AN.start_position)) AND         "
					+ "(ABS(VCF.end_position-AN.end_position) >= ABS(VCF.start_position - AN.start_position)))          "
					+ "THEN ABS(VCF.start_position-AN.start_position)         WHEN ((ABS(VCF.end_position-AN.start_position) >= ABS(VCF.end_position - AN.end_position)))          "
					+ "THEN ABS(VCF.end_position-AN.end_position)         ELSE ABS(VCF.end_position-AN.start_position) END) " + "    LIMIT "
					+ "      1 )[SAFE_OFFSET(0)] names " + ", CASE " + "		 WHEN "
					+ "        ((AN.start_position >  VCF.end_position) OR ( VCF.start_position > AN.end_position)) " + "        THEN "
					+ "        'Closest' " + "        ELSE " + "        'Overlapped'" + "       END AS Status "
					+ " FROM " + VCFQuery + " JOIN `" + TableName + "` AS AN " + "  ON VCF.reference_name = AN.reference_name ";
			if (onlyIntrogenic)
				Query += " WHERE (VCF.start_position>AN.end_position) OR (AN.start_position> VCF.end_position) ";
			Query += " GROUP BY VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, alternate_bases, Status";

			// :TODO M and MT case
		} else {

			Query = "  SELECT " + " VCF.reference_name as reference_name, VCF.start_position as start_position, VCF.end_position, "
					+ "VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, " + AllFields
					+ ", AN_start" + ", AN_end" + ", CASE"
					+ "      WHEN (ABS(VCF.end_position-AN_Start)>=ABS(VCF.start_position - AN_End)) THEN ABS(VCF.start_position-AN_End)"
					+ "      ELSE ABS(VCF.end_position-AN_Start)" + "    END AS distance" + ", CASE "
					+ "    WHEN ((VCF.start_position <= AN_End) AND (AN_Start <= VCF.end_position)) THEN 'Overlapped' "
					+ "    ELSE 'Closest' " + "  END AS Status " + " FROM " + VCFQuery + " JOIN (" + "SELECT "
					+ "      reference_name " + "      , start_position as AN_Start  " + "      , `end_position` as AN_END " + RequestedFields
					+ " From " + "`" + TableName + "`) AS AN " + " ON AN.reference_name = VCF.reference_name ";
			// NOTE: It finds all genes within the range irrespective of whether a variant
			// overlapped w/ any gene or not.
			/*
			 * if (onlyIntrogenic) { Query +=
			 * " WHERE (( VCF.start> AN.End) OR (AN.Start> VCF.END))"; }
			 */

			Query = "Select * from (" + Query + ") WHERE distance < " + Threashold;
		}

		return Query;
	}



	public static void listAnnotationDatasets( String datasetId, String tableId, String annotationType,
			String annotationDatasetBuild, boolean printAll, String searchKeyword) throws InterruptedException {
				
		String queryStat ="";
		
		String Fields=" AnnotationSetName, AnnotationSetType, AnnotationSetSize, Build, AnnotationSetFields ";
		if (printAll) {
			Fields=" * ";
		}
		
		if (!annotationDatasetBuild.isEmpty())
		{
			queryStat ="SELECT "+ Fields + " from [" + datasetId + "." + tableId + "] "
					+ "WHERE Build=\"" + annotationDatasetBuild + "\"";
		}
		
		if (!searchKeyword.isEmpty()) {
			if (queryStat.isEmpty()) {
			queryStat ="SELECT "+ Fields + " from ["  + datasetId + "." + tableId + "] "
					+ "WHERE lower(AnnotationSetName) LIKE \"%" + searchKeyword.toLowerCase() + "%\"";
			}else 
				queryStat += " AND lower(AnnotationSetName) LIKE \"%" + searchKeyword.toLowerCase() + "%\"";
			
		}
		
		if(!annotationType.isEmpty()) {
			if (queryStat.isEmpty())
				queryStat ="SELECT "+ Fields + " from ["  + datasetId + "." + tableId + "] "
					+ "WHERE AnnotationSetType = " + annotationType;
			else
				queryStat += " AND AnnotationSetType = \"" + annotationType + "\"";

		}
		
		if (annotationType.isEmpty() && searchKeyword.isEmpty() && annotationDatasetBuild.isEmpty()){
			queryStat ="SELECT "+ Fields + "  from [" + datasetId + "." + tableId + "] "
					+ " Order By Build";
		}
		
		// Get the results.
		//System.out.println("Stat Query: " + queryStat);
		QueryResponse response = runquery(queryStat);
		QueryResult result = response.getResult();
		System.out.println("\n\n<============ AnnotationList ============>");
		if (result!=null) {
		    System.out.format("%-80s %-10s %-15s %-10s %s",
		    		"Name",
		    		"Type", 
		    		"Size", 
		    		"Build", 
		    		"Fields");
	        System.out.println();
		}
		
		while (result != null) {		
			for (List<FieldValue> row : result.iterateAll()) {

			    System.out.format("%-80s %-10s %-15s %-10s %s",
			    		row.get(0).getStringValue(), 
			    		row.get(1).getStringValue(), 
			    		row.get(2).getStringValue(), 
			    		row.get(3).getStringValue(), 
			    		row.get(4).getStringValue());
			    
		        System.out.println();
		        
			}
			result = result.getNextPage();
		}
		
	}

	public static String prepareOneVariantQuery_StandardSQL(String[] va, String VariantAnnotationTableNames,
			String VariantannotationCanonicalizerefNames) {
		
		String[] VariantAnnotationTables=null;
		String[] VariantannotationCanonicalize=null;

		
		////////////Variant Annotations///////
		if(VariantAnnotationTableNames!=null){
			VariantAnnotationTables = VariantAnnotationTableNames.split(","); 
			
			if(!VariantannotationCanonicalizerefNames.isEmpty()){
				VariantannotationCanonicalize = VariantannotationCanonicalizerefNames.split(","); 
				if (VariantannotationCanonicalize.length != VariantAnnotationTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
		}
		

				
		/*#######################Prepare VCF queries#######################*/
		 	 
		String AllFields=""; // Use to 
		String AnnotationQuery="";
		int AnnotationIndex=1; // Use for creating alias names
		
		String reference_name= "\"" + va[0] + "\"";
		int start= (Integer.parseInt(va[1])-1)  ; //0-base
		int end=  Integer.parseInt(va[2]);
		String RefBases= "\"" + va[3] + "\"";
		String AltBase= "\"" + va[4] + "\"";
		
		
		/*#######################Prepare VCF queries#######################*/
		if(VariantAnnotationTables!=null){
			for (int index=0; index< VariantAnnotationTables.length; index++,AnnotationIndex++){
						
						/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
						 * ProjectId: gbsc-gcp-project-cba
						 * DatasetId: PublicAnnotationSets
						 * TableId: hg19_GME
						 * Features: GME_AF:GME_NWA:GME_NEA
						 */
						
						String [] TableInfo = VariantAnnotationTables[index].split(":");
												
						String RequestedFields="";
						String AliasTableName= "Annotation" + AnnotationIndex; 
						String TableName = TableInfo[0]+":"+TableInfo[1];
						
						
						for (int fieldIndex=2; fieldIndex<TableInfo.length; fieldIndex++){
							if (TableInfo.length>3){ // Creating CONCAT
								if (fieldIndex+1 < TableInfo.length)
									RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" , \"/\" ,";
								else
									RequestedFields += AliasTableName +"." + TableInfo[fieldIndex];
							}
							else //If there is only one feature
								RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] ;
							//TODO: Limited to 64 databases 
							if(fieldIndex==2){
	
								if (TableInfo.length>3){ //Top Select 
									//(e.g.,     CONCAT(Annotation3.name, "/",Annotation3.name2) AS Annotation3.Annotation3)
									//AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
									AllFields += ", max(" + AliasTableName +"." + AliasTableName + ") as " + TableInfo[1] +"." + TableInfo[fieldIndex] + " ";
									//AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + AliasTableName + " ) ";
								}
								else{
									//AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
									AllFields += ", max("+ AliasTableName +"." + TableInfo[fieldIndex] + ") as " + TableInfo[1] +"." + TableInfo[fieldIndex] + " ";

									//AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
								}
							}										
						}
						
						//IF the number of fields is more that 1 -> then concat all of them
						if (TableInfo.length>3)
							AnnotationQuery += " ( SELECT " + reference_name + ", " + start + "," + end + ", " + RefBases + ","+ AltBase + ", "
									+ "CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
						else
							AnnotationQuery +=  " ( SELECT " + reference_name + ", " + start + "," + end + ", " + RefBases + ","+ AltBase + ", "
								    + RequestedFields;

						AnnotationQuery +=
							 " FROM [" + TableName +"] AS " + AliasTableName ;
						AnnotationQuery += " WHERE (" + AliasTableName + ".reference_name = " + reference_name + ") "
							+ " AND (" + AliasTableName + ".start_position = " + start + ") AND (" + AliasTableName + ".end_position = " + end + ") "
							+ " AND (((CONCAT(" + RefBases.replace("-", "") + ", " + AliasTableName + ".alternate_bases) =" + AltBase.replace("-", "") +" ) "
							+ " OR " + AliasTableName + ".alternate_bases = " + AltBase.replace("-", "") + ") AND (" + RefBases.replace("-", "") +" = " + AliasTableName + ".reference_bases)) ";
						
						//This is the case when we have transcript annotations 
						if(index+1 <  VariantAnnotationTables.length)
							AnnotationQuery +=  "), ";
						else
							AnnotationQuery +=  ") ";
			}

		}

		String Query= "  SELECT "
				+ reference_name + " as reference_name, " + start + " as start_position, "+ end + " as end_position, " + RefBases + " as reference_bases, "
				+   AltBase + " as alternate_bases " + AllFields 
				+ " FROM " + AnnotationQuery;
					
		Query += " GROUP BY  reference_name, start_position, end_position, reference_bases, alternate_bases " ; //+ AllFields; 
	
		
		return Query;
	}

	public static String prepareOneRegionQuery_StandardSQL(String[] region, String TranscriptAnnotationTableNames,
			  String TranscriptCanonicalizeRefNames) {
		
		 
			
	String[] TranscriptAnnotations=null;
	String[] TranscriptCanonicalize=null;

	/////////////////////Transcripts//////////////
	if(TranscriptAnnotationTableNames!=null){
		TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 

		if(!TranscriptCanonicalizeRefNames.isEmpty()){
			TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
			if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
				throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
		}
	}

		String AllFields=""; // Use to 
		String AnnotationQuery="";
		int AnnotationIndex=1; // Use for creating alias names
		
		String reference_name= "\"" + region[0] + "\"";
		int start= (Integer.parseInt(region[1])-1)  ; //0-base
		int end=  Integer.parseInt(region[2]);
		
			
		/*#######################Prepare VCF queries#######################*/
		if(TranscriptAnnotations!=null){
			for (int index=0; index< TranscriptAnnotations.length; index++, AnnotationIndex++){
						
						/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
						 * ProjectId: gbsc-gcp-project-cba
						 * DatasetId: PublicAnnotationSets
						 * TableId: hg19_GME
						 * Features: GME_AF:GME_NWA:GME_NEA
						 */
						
						String [] TableInfo = TranscriptAnnotations[index].split(":");
												
						String RequestedFields="";
						String AliasTableName= "Annotation" + AnnotationIndex; 
						String TableName = TableInfo[0]+":"+TableInfo[1];
						
						
						for (int fieldIndex=2; fieldIndex<TableInfo.length; fieldIndex++){
							if (TableInfo.length>3){ // Creating CONCAT
								if (fieldIndex+1 < TableInfo.length)
									RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" , \"/\" ,";
								else
									RequestedFields += AliasTableName +"." + TableInfo[fieldIndex];
							}
							else //If there is only one feature
								RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] ;
							//TODO: Limited to 64 databases 
							if(fieldIndex==2){
	
								if (TableInfo.length>3){ //Top Select 
									//(e.g.,     CONCAT(Annotation3.name, "/",Annotation3.name2) AS Annotation3.Annotation3)
									//AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
									AllFields += ", max(" + AliasTableName +"." + AliasTableName + ") as " + TableInfo[1] +"." + TableInfo[fieldIndex] + " ";
									//AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + AliasTableName + " ) ";
								}
								else{
									//AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
									AllFields += ", max("+ AliasTableName +"." + TableInfo[fieldIndex] + ") as " + TableInfo[1] +"." + TableInfo[fieldIndex] + " ";

									//AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
								}
							}
														
						}
						
						//IF the number of fields is more that 1 -> then concat all of them
						if (TableInfo.length>3)
							AnnotationQuery += " ( SELECT " + reference_name + ", " + start + "," + end + ", "
									+ "CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
						else
							AnnotationQuery +=  " ( SELECT " + reference_name + ", " + start + "," + end + ", "
								    + RequestedFields;

						AnnotationQuery +=
							 " FROM [" + TableName +"] AS " + AliasTableName ;
						AnnotationQuery += " WHERE (" + AliasTableName + ".reference_name = " + reference_name + ") "
								+ " AND ("+ AliasTableName +".start_position <= " + end +") AND (" + start + "<= "+ AliasTableName +".end_position ) ";				
				
									
						//This is the case when we have transcript annotations 
						if(index+1 <  TranscriptAnnotations.length)
							AnnotationQuery +=  "), ";
						else
							AnnotationQuery +=  ") ";
			}

		}

		String Query= "  SELECT "
				+ reference_name + " as reference_name, " + start + " as start_position, "+ end + " as end_position " + AllFields 
				+ " FROM " + AnnotationQuery;
					
		Query += " GROUP BY  reference_name, start_position, end_position " ; //+ AllFields; 
	
		
		return Query;
	}
	

	public static String createTempVCFTable(List<String[]> listVA, String TempVCFTable, boolean VariantBased) {
		
		String Query="WITH "+ TempVCFTable +" AS ( ";
		boolean setFields=true;
		for(int index=0; index<listVA.size(); index++)
		{
			String [] va = listVA.get(index);
			String reference_name= "\"" + va[0] + "\"";
			int start= (Integer.parseInt(va[1])-1)  ; //Convert to 0-based
			int end=  Integer.parseInt(va[2]);
					
			if (setFields) {
				Query += "SELECT " + reference_name + " reference_name, " + start + " start_position, " + end + " `end_position` ";
				if (VariantBased) {
					String RefBases= "\"" + va[3] + "\"";
					String AltBase= "\"" + va[4] + "\"";
					Query += ", " + RefBases + " reference_bases ," +  AltBase + " alternate_bases ";
				}
				setFields= false;
			}
			else {
				Query += " \n SELECT " + reference_name + ", " + start + ", " + end;
				if (VariantBased) {
					String RefBases= "\"" + va[3] + "\"";
					String AltBase= "\"" + va[4] + "\"";
					Query += ", " + RefBases + "," +  AltBase;

				}				
			}
			if (index+1!=listVA.size())
				Query += " UNION ALL ";
		}
		if (VariantBased) {
			Query += ") \nSELECT reference_name, start_position, `end_position`, reference_bases, alternate_bases FROM " + TempVCFTable;
		}
		else {
			Query += ") \nSELECT reference_name, start_position, `end_position` FROM " + TempVCFTable;
			
		}
		return Query;
	}	
	
	
	public static String prepareAnnotateVariantQueryConcatFields_mVCF_GroupBy(String VCFTableNames, String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			  String TranscriptCanonicalizeRefNames, String VariantAnnotationTableNames, 
			  String VariantannotationCanonicalizerefNames, boolean TempVCF, boolean GroupBy, 
			  String searchRegions) {		
				
		String[] TranscriptAnnotations=null;
		String[] VariantAnnotationTables=null;
		String[] VCFTables=null;
		String[] VCFCanonicalize=null; 
		String[] TranscriptCanonicalize=null;
		String[] VariantannotationCanonicalize=null;
	
		int numAnnotationDatasets=0;
		/////////////////////Transcripts//////////////
		if(TranscriptAnnotationTableNames!=null){
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
			numAnnotationDatasets=+ TranscriptAnnotations.length;
			if(!TranscriptCanonicalizeRefNames.isEmpty()){
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
				
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}
		
		////////////Variant Annotations///////
		if(VariantAnnotationTableNames!=null){
			VariantAnnotationTables = VariantAnnotationTableNames.split(","); 
			numAnnotationDatasets=+ VariantAnnotationTables.length;

			if(!VariantannotationCanonicalizerefNames.isEmpty()){
				VariantannotationCanonicalize = VariantannotationCanonicalizerefNames.split(","); 
				if (VariantannotationCanonicalize.length != VariantAnnotationTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
		}
		
		//
		if(numAnnotationDatasets<=64)
			GroupBy=true;
		else
			GroupBy=false;
		
		//////////VCF Files/////////////
		/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
		if(!VCFTableNames.isEmpty()){
			VCFTables = VCFTableNames.split(","); 
		
			if(!VCFCanonicalizeRefNames.isEmpty()){
				VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
			else{
				System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}
		
		String WHERE_VCF=" WHERE ";
		String WHERE_ANN=" WHERE ";
		
		if (!searchRegions.isEmpty()) {
			String[] temp = searchRegions.split(",");
			for (int index=0; index <temp.length; index++) {
				String [] RS = temp[index].split(":");
				if(index+1<temp.length) {
					WHERE_VCF += " (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start_position > " + RS[1] + " AND start_position < " + RS[2] + ") OR " ;
					
					WHERE_ANN += " (reference_name=\"" + RS[0].replace("chr", "")
									+ "\") OR " ;
				}
				else {
					WHERE_VCF += " (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start_position > " + RS[1] + " AND start_position < " + RS[2] + ") " ;
					
					WHERE_ANN += " (reference_name=\"" + RS[0].replace("chr", "") 
									+ "\")" ;
				}
			}
			
		}
		
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index< VCFTables.length; index++){
			if(VCFCanonicalize!=null)
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, end_position, reference_bases, alternate_bases, call.call_set_name, quality  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, end_position, reference_bases, alternate_bases ";

			if (index+1<VCFTables.length) {
					VCFQuery += " FROM ["+ VCFTables[index]  +"] ";				
					if (!searchRegions.isEmpty()) {
						VCFQuery += WHERE_VCF;
					}
					VCFQuery += "OMIT RECORD IF EVERY(call.genotype <= 0) ), ";  
				}
			else {
					VCFQuery += " FROM ["+ VCFTables[index]  +"] ";
					if (!searchRegions.isEmpty()) {
						VCFQuery += WHERE_VCF;
					}
					VCFQuery	 += "OMIT RECORD IF EVERY(call.genotype <= 0) )";
				}
		}
		VCFQuery +=" ) as VCF ";

		 	 
		String AllFields=""; // Use to 
		String Concat_Group=""; // Use to 

		String AnnotationQuery="";
		int AnnotationIndex=1; // Use for creating alias names
		
		/*#######################Prepare VCF queries#######################*/
		if(VariantAnnotationTables!=null){
			for (int index=0; index< VariantAnnotationTables.length; index++,AnnotationIndex++){
						
						/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
						 * ProjectId: gbsc-gcp-project-cba
						 * DatasetId: PublicAnnotationSets
						 * TableId: hg19_GME
						 * Features: GME_AF:GME_NWA:GME_NEA
						 */
						
						String [] TableInfo = VariantAnnotationTables[index].split(":");
												
						String RequestedFields="";
						String AliasTableName= "Annotation" + AnnotationIndex; 
						String TableName = TableInfo[0]+":"+TableInfo[1];
						
						
						for (int fieldIndex=2; fieldIndex<TableInfo.length; fieldIndex++){
							if (TableInfo.length>3){ // Creating CONCAT
								if (fieldIndex+1 < TableInfo.length)
									RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" , \"/\" ,";
								else
									RequestedFields += AliasTableName +"." + TableInfo[fieldIndex];
							}
							else //If there is only one feature
								RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] ;

							if(fieldIndex==2){
	
								if (TableInfo.length>3){ //Top Select 
									//(e.g.,     CONCAT(Annotation3.name, "/",Annotation3.name2) AS Annotation3.Annotation3)
									if (GroupBy) {
										AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " )  as AN"+Integer.toString(index+1) + " ";
										Concat_Group +=", GROUP_CONCAT(AN" + Integer.toString(index+1) + ")";
									}
									else {
										AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
									}
									//AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + AliasTableName + " ) ";
								}
								else{
									if (GroupBy) {
										AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) as AN"+Integer.toString(index+1) + " ";
										Concat_Group +=", GROUP_CONCAT(AN" + Integer.toString(index+1) + ")";
									}
									else {
										AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
									}
										//AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
								}
							}
						}
						
						//IF the number of fields is more that 1 -> then concat all of them
						if (TableInfo.length>3)
							AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, "
									+ "CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
						else
							AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, "
								    + RequestedFields;

						AnnotationQuery +=" FROM " + VCFQuery;
						
						if (!searchRegions.isEmpty()) {
							AnnotationQuery += " JOIN (SELECT * FROM [" + TableName +"] " 
							+ WHERE_ANN + ") AS " + AliasTableName ;
						}else {
							AnnotationQuery +=" JOIN [" + TableName +"] AS " + AliasTableName ;
						}
						AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start_position = VCF.start_position) AND (" + AliasTableName + ".end_position = VCF.end_position) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alternate_bases) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alternate_bases = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".reference_bases)) ";
						
						//This is the case when we have transcript annotations 
						if(index+1 <  VariantAnnotationTables.length || (TranscriptAnnotations!=null))
							AnnotationQuery +=  "), ";
						else
							AnnotationQuery +=  ") ";
			}
		}
		
		 
		if(TranscriptAnnotations!=null){
			for (int index=0; index< TranscriptAnnotations.length; index++, AnnotationIndex++){

				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */
				String [] TableInfo = TranscriptAnnotations[index].split(":");
								
				String RequestedFields="";
				String AliasTableName= "Annotation" + AnnotationIndex; 
				String TableName = TableInfo[0]+":"+TableInfo[1];
				
				
				for (int fieldIndex=2; fieldIndex<TableInfo.length; fieldIndex++){
					if (TableInfo.length>3){ // Creating CONCAT
						if (fieldIndex+1 < TableInfo.length)
							RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" , \"/\" ,";
						else
							RequestedFields += AliasTableName +"." + TableInfo[fieldIndex];
					}
					else //If there is only one feature
						RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] ;
					
					if(fieldIndex==2){
						if (TableInfo.length>3){ //Top Select 
							if(VariantAnnotationTables!=null)
								AllFields += ", CONCAT(\""+ (index+1+VariantAnnotationTables.length) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
							else {
								if (GroupBy) {
									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " )  as AN"+Integer.toString(index+1) + " ";
									Concat_Group +=", GROUP_CONCAT(AN" + Integer.toString(index+1) + ")";
								}
								else {
									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
								}
							}
						}
						else{
							if(VariantAnnotationTables!=null)
								AllFields += ", CONCAT(\""+ (index+1+VariantAnnotationTables.length) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
							else	{
								if (GroupBy) {
									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) as AN"+Integer.toString(index+1) + " ";
									Concat_Group +=", GROUP_CONCAT(AN" + Integer.toString(index+1) + ")";
								}
								else {
									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
								}
							}
						}
						
					}												
				}

				//IF the number of fields is more that 1 -> then concat all of them
				if (TableInfo.length>3)
					AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, "
							+ "VCF.alternate_bases, CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
				else
					AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, "
						    + RequestedFields;

				AnnotationQuery +=" FROM " + VCFQuery;
					 
				if (!searchRegions.isEmpty()) {
					AnnotationQuery += " JOIN (SELECT * FROM [" + TableName +"] " 
					+ WHERE_ANN + ") AS " + AliasTableName ;
				}else {
						AnnotationQuery += " JOIN [" + TableName +"] AS " + AliasTableName ;
				}
						
				AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";


				AnnotationQuery += " WHERE "
						+ " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) ";				
				
				if(index+1 <  TranscriptAnnotations.length)
					AnnotationQuery +=  "), ";
				else
					AnnotationQuery +=  ") ";
			
			}
		}		
	
		String Query= "  SELECT "
				+ " VCF.reference_name as reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases as reference_bases, "
				+ "VCF.alternate_bases as alternate_bases " + AllFields 
				+ " FROM ";
		
		Query += AnnotationQuery;
		
		if(GroupBy) {
			Query = " SELECT    reference_name, start_position, end_position, reference_bases, alternate_bases " + Concat_Group 
					+ " FROM ( " + Query
					+ " ) GROUP BY  reference_name, start_position, end_position, reference_bases, alternate_bases " ; 
		}
		return Query;
	}

	public static void incrementStart(String projectId, String bigQueryDatasetId, String outputBigQueryTable) throws Exception  {
	
		String queryStat = "UPDATE   " + "`" + projectId + "."
				+ bigQueryDatasetId + "." + outputBigQueryTable + "` "
				+ " SET start_position = start_position + 1 WHERE reference_name <>\"\" ";
		QueryResponse response = runquery(queryStat);
	}

	
	
	public static String prepareAnnotateVariantQueryRegion_Name(String VCFTableNames, String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			  String TranscriptCanonicalizeRefNames, String VariantAnnotationTableNames, String VariantannotationCanonicalizerefNames, String SampleNames, boolean GroupBy, boolean customizedVCF, 
			  String searchRegions, boolean GoogleVCF) {

		String[] TranscriptAnnotations=null;
		String[] VariantAnnotationTables=null;
		String[] VCFTables=null;
		String[] VCFCanonicalize=null; 
		String[] TranscriptCanonicalize=null;
		String[] VariantannotationCanonicalize=null;
	
		/////////////////////Transcripts//////////////
		if(TranscriptAnnotationTableNames!=null){
			TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
	
			if(!TranscriptCanonicalizeRefNames.isEmpty()){
				TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
				if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
			}
		}
		////////////Variant Annotations///////
		if(VariantAnnotationTableNames!=null){
			VariantAnnotationTables = VariantAnnotationTableNames.split(","); 
			
			if(!VariantannotationCanonicalizerefNames.isEmpty()){
				VariantannotationCanonicalize = VariantannotationCanonicalizerefNames.split(","); 
				if (VariantannotationCanonicalize.length != VariantAnnotationTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
		}
		
		//////////VCF Files/////////////
		/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
		if(!VCFTableNames.isEmpty()){
			VCFTables = VCFTableNames.split(","); 
		
			if(!VCFCanonicalizeRefNames.isEmpty()){
				VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
				if (VCFCanonicalize.length != VCFTables.length)
					throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
			}
			else{
				System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
			}
		}
		
		String WHERE_VCF=" WHERE ";
		String WHERE_ANN=" WHERE ";
		
		if (!searchRegions.isEmpty()) {
			String[] temp = searchRegions.split(",");
			for (int index=0; index <temp.length; index++) {
				String [] RS = temp[index].split(":");
				if(index+1<temp.length) {
					WHERE_VCF += " (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start_position > " + RS[1] + " AND start_position < " + RS[2] + ") OR " ;
					
					WHERE_ANN += " (reference_name=\"" + RS[0].replace("chr", "")
									+ "\") OR " ;
				}
				else {
					WHERE_VCF += " (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start_position > " + RS[1] + " AND start_position < " + RS[2] + ") " ;
					
					WHERE_ANN += " (reference_name=\"" + RS[0].replace("chr", "") 
									+ "\")" ;
				}
			}
			
		}
				
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index < VCFTables.length; index++){
			if(VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, end_position ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, end_position ";

			//if(VariantAnnotationTableNames!=null){
				VCFQuery += ", reference_bases, alternate_bases";
			//}

				
			if(!SampleNames.isEmpty())
				VCFQuery +=  ", call.call_set_name  ";
			
			if (!customizedVCF && GoogleVCF) {
				if (index+1<VCFTables.length) {
					VCFQuery += " FROM ["+ VCFTables[index]  +"] ";				
					if (!searchRegions.isEmpty()) {
						VCFQuery += WHERE_VCF;
					}
					VCFQuery += "OMIT RECORD IF EVERY(call.genotype <= 0) ), ";  
				}
			else {
					VCFQuery += " FROM ["+ VCFTables[index]  +"] ";
					if (!searchRegions.isEmpty()) {
						VCFQuery += WHERE_VCF;
					}
					VCFQuery	 += "OMIT RECORD IF EVERY(call.genotype <= 0) )";
				}
			}else //CustomizedVCF OR those imported by AnnotationHive
			{
					VCFQuery += " FROM ["+ VCFTables[index]  +"] )) as VCF ";
			}
		}
		if (!customizedVCF && GoogleVCF) {
			//VCFQuery +=" where call.call_set_name = \""+ SampleNames + "\") as VCF ";
			VCFQuery +="  ) as VCF ";
		}
		 	 
		String AllFields=""; // Use to 
		String AnnotationQuery="";
		int AnnotationIndex=1; // Use for creating alias names
		
		/*#######################Prepare VCF queries#######################*/
		if(VariantAnnotationTables!=null){
			for (int index=0; index< VariantAnnotationTables.length; index++,AnnotationIndex++){
						
						/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
						 * ProjectId: gbsc-gcp-project-cba
						 * DatasetId: PublicAnnotationSets
						 * TableId: hg19_GME
						 * Features: GME_AF:GME_NWA:GME_NEA
						 */
						
						String [] TableInfo = VariantAnnotationTables[index].split(":");
												
						String RequestedFields="";
						String AliasTableName= "Annotation" + AnnotationIndex; 
						String TableName = TableInfo[0]+":"+TableInfo[1];
						
						for (int index2=2; index2<TableInfo.length; index2++){
							RequestedFields +=  " , " + AliasTableName +"." + TableInfo[index2] ;
							if(index2==2 && !GroupBy){
//								if (GroupBy)
//									AllFields += ", MAX ( CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[index2] + " )) ";
//								else
									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[index2] + " ) ";
									//AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[index2] + " ) ";

								//CONCAT("hg19_cosmic70: ",Annotation1.CosmicID )
							}
							else{
								if (GroupBy)
									AllFields +=  " , MAX( " + AliasTableName +"." + TableInfo[index2] + ") as " + TableInfo[1].split("\\.")[1] +"_"+ TableInfo[index2];
								else
									AllFields +=  " , " + AliasTableName +"." + TableInfo[index2];
							}
						}
												
						AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases " + RequestedFields
							+ " FROM " + VCFQuery;
						
						if (!searchRegions.isEmpty()) {
							AnnotationQuery += " JOIN (SELECT * FROM [" + TableName +"] " 
							+ WHERE_ANN + ") AS " + AliasTableName ;
						}else {
							AnnotationQuery +=" JOIN [" + TableName +"] AS " + AliasTableName ;
						}
						
						AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start_position = VCF.start_position) AND (" + AliasTableName + ".end_position = VCF.end_position) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alternate_bases) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alternate_bases = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".reference_bases)) ";
									//+ "GROUP BY VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases " + RequestedFields;
						if(index+1 <  VariantAnnotationTables.length || (TranscriptAnnotations!=null))
							AnnotationQuery +=  "), "; //In case there are more variant annotations OR we have  
						else
							AnnotationQuery +=  ") ";
						
						
			}
		}
		
		 
		if(TranscriptAnnotations!=null){
				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */

			for (int index=0; index< TranscriptAnnotations.length; index++,AnnotationIndex++){
				
				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
				 * ProjectId: gbsc-gcp-project-cba
				 * DatasetId: PublicAnnotationSets
				 * TableId: hg19_refGene_chr17
				 * Features: exonCount:exonStarts:exonEnds:score
				 */
				
				String [] TableInfo = TranscriptAnnotations[index].split(":");
										
				String RequestedFields="";
				String AliasTableName= "Annotation" + AnnotationIndex; 
				String TableName = TableInfo[0]+":"+TableInfo[1];
				
				for (int index2=2; index2<TableInfo.length; index2++){
					RequestedFields +=  " , " + AliasTableName +"." + TableInfo[index2] ;
					if(index2==2 && !GroupBy){
						AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[index2] + " ) ";
					}
					else{
						if (GroupBy)
							AllFields +=  " , MAX( " + AliasTableName +"." + TableInfo[index2] + ") as " + TableInfo[1].split("\\.")[1] +"_"+ TableInfo[index2];
						else
							AllFields +=  " , " + AliasTableName +"." + TableInfo[index2];
					}
				}
										
				AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start_position, VCF.end_position ";
					//	if(VariantAnnotationTableNames!=null){
							AnnotationQuery += ", VCF.reference_bases, VCF.alternate_bases " ;
					//	}
				AnnotationQuery +=RequestedFields ;

				AnnotationQuery +=" FROM " + VCFQuery;
				 
				if (!searchRegions.isEmpty()) {
					AnnotationQuery += " JOIN (SELECT * FROM [" + TableName +"] " 
					+ WHERE_ANN + ") AS " + AliasTableName ;
				}else {
						AnnotationQuery += " JOIN [" + TableName +"] AS " + AliasTableName ;
				}
						
				AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";
				
				
				AnnotationQuery += " WHERE "			
				//		x1 <= y2 && y1 <= x2
				+ "((("+ AliasTableName +".start_position <= VCF.end_position) AND ("+ AliasTableName +".end_position >= VCF.start_position)))";

		
				if(index+1 <  TranscriptAnnotations.length)
					AnnotationQuery +=  "), ";
				else
					AnnotationQuery += ") ";
			}
		
		}		
	
		String Query= "  SELECT VCF.reference_name as reference_name, VCF.start_position, VCF.end_position ";
				//if(VariantAnnotationTableNames!=null){
					 Query+= ", VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases ";
				//}
				
		Query += AllFields + " FROM ";
		
		Query += AnnotationQuery;
		if (GroupBy) {
			Query += " GROUP BY  reference_name, start_position, end_position "; 
			
			//if(VariantAnnotationTableNames!=null){
				Query += ", reference_bases, alternate_bases";
			//}
		}
		return Query;
	}




	public static void findNumSamples_StandardSQL(String project, String bigQueryDatasetId, String bigQueryVCFTableId,
			String numSampleIDs, String Header) {

		 
		boolean LegacySql = false;
		boolean Update = false; 
		boolean allowLargeResults = false;
		int maximumBillingTier= 0;
		boolean DDL = false;
		
		/*String otherFields="";
		 
		if (!Header.isEmpty()) {
				String[] inputFields= Header.split(",");
				for(int i=5; i<inputFields.length; i++){
					if( i+1< inputFields.length)
						otherFields += inputFields[i] + ",";
					else
						otherFields += inputFields[i];
				}
		}*/
		 
		String tempTableName = "`" + project + "." + bigQueryDatasetId + "."
				+ bigQueryVCFTableId +"`";
		
		String outputBigQueryTable= bigQueryVCFTableId+"_With_SampleCount"; 
		String[] samples = numSampleIDs.split(",");
		//String queryString = "SELECT  reference_name as reference_name, start, `end`, reference_bases as base, alternate_bases as alt, ID, QUAL, FILTER, INFO, FORMAT ";
		String queryString = "SELECT  reference_name, start_position, end_position, reference_bases, alternate_bases, ID, QUAL, FILTER, INFO, FORMAT ";

		for (int index=0; index<samples.length; index++) {
			queryString += "," + samples[index]; 
		}
		 
		
		//String queryString = "SELECT  reference_name as reference_name, start, `end`, reference_bases as base,   ARRAY_AGG(alt) as alt, ";
		
		 
		String SNames = ", concat( ";
		String SCount = "CAST ( ";
		for (int index=0; index<samples.length; index++) {
			if (index+1<samples.length) {
				SNames += " CASE WHEN " + samples[index] + " not like \".%\" Then \"" +  samples[index] + " \" ELSE \"\" END , ";
				SCount += " CASE WHEN " + samples[index] + " not like \".%\" Then 1 ELSE 0 END + ";
			}else {
				SNames += " CASE WHEN " + samples[index] + " not like \".%\" Then \" " +  samples[index] + "\" ELSE \"\" END ) as Sample_Names, ";
				SCount += " CASE WHEN " + samples[index] + " not like \".%\" Then 1 ELSE 0 END as STRING) as Num_Samples ";
			}
		}
		 
		 
		 queryString += SNames + SCount; // + ", " + otherFields;
		 queryString += " FROM " + tempTableName; // + ", UNNEST (alternate_bases) as alt ";
		 //queryString += "Group By reference_name, start, `end`, base, " + otherFields + ", Sample_Names, Num_Samples";
		 
		LOG.info(queryString);		 
		        
		try {
			BigQueryFunctions.runQueryPermanentTable(project, queryString, bigQueryDatasetId, outputBigQueryTable,
					allowLargeResults, maximumBillingTier, LegacySql, Update, DDL);
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.info("To use " + outputBigQueryTable + " add it to the list of annotation in --variantAnnotationTables (e.g., --variantAnnotationTables=" + project  + ":" + bigQueryDatasetId + "." + bigQueryVCFTableId + "_With_SampleCount:Sample_Names:Num_Samples)");		 

		
	}
	
	
public static String prepareAnnotateVariantQueryConcatFields_mVCF_StandardSQL(String VCFTableNames, 
		String VCFCanonicalizeRefNames, String UCSCTranscriptAnnotationTableNames, String TranscriptAnnotationTableNames,
		  String TranscriptCanonicalizeRefNames, String VariantAnnotationTableNames, 
		  String VariantannotationCanonicalizerefNames, 
		  boolean createVCF, boolean TempVCF, boolean GoogleVCF, boolean numSamples, 
		  String build, boolean LeftJoin, int binSize) {

	System.out.println("bin size: " + binSize );
	
	String[] UCSCTranscriptAnnotations=null;		
	String[] TranscriptAnnotations=null;
	String[] VariantAnnotationTables=null;
	String[] VCFTables=null;
	String[] VCFCanonicalize=null; 
	String[] TranscriptCanonicalize=null;
	String[] VariantannotationCanonicalize=null;

	ArrayList<Integer> allAnnotationParams = new ArrayList<Integer>();
	ArrayList<String []> allAnnotationParamsValues = new ArrayList<String []>();

	int numAnnotationDatasets=0;
	
	////////////Variant Annotations///////
	if(VariantAnnotationTableNames!=null){
		VariantAnnotationTables = VariantAnnotationTableNames.split(","); 
		numAnnotationDatasets=+ VariantAnnotationTables.length;

		for (int index=0; index<VariantAnnotationTables.length; index++) {
			allAnnotationParams.add(VariantAnnotationTables[index].split(":").length-2);
			allAnnotationParamsValues.add(VariantAnnotationTables[index].split(":"));
		}

		if(!VariantannotationCanonicalizerefNames.isEmpty()){
			VariantannotationCanonicalize = VariantannotationCanonicalizerefNames.split(","); 
			if (VariantannotationCanonicalize.length != VariantAnnotationTables.length)
				throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
		}
	}
	
	/////////////////////Transcripts//////////////
	if(TranscriptAnnotationTableNames!=null){
		TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
		numAnnotationDatasets=+ TranscriptAnnotations.length;
		for (int index=0; index<TranscriptAnnotations.length; index++) {			
			allAnnotationParams.add(TranscriptAnnotations[index].split(":").length-2);
			allAnnotationParamsValues.add(TranscriptAnnotations[index].split(":"));
		}
		if(!TranscriptCanonicalizeRefNames.isEmpty()){
			TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
			
			if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
				throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
		}
	}

	
	/////////////////////UCSCTranscripts//////////////
	if(UCSCTranscriptAnnotationTableNames!=null){
		UCSCTranscriptAnnotations = UCSCTranscriptAnnotationTableNames.split(","); 
		numAnnotationDatasets=+ UCSCTranscriptAnnotations.length;
		for (int index=0; index<UCSCTranscriptAnnotations.length; index++) {			
			allAnnotationParams.add(UCSCTranscriptAnnotations[index].split(":").length-2);
			allAnnotationParamsValues.add(UCSCTranscriptAnnotations[index].split(":"));
		}
	}
	
	//BigQuery Limit
	boolean GroupBy;
	if(numAnnotationDatasets<=64)
		GroupBy=true;
	else
		GroupBy=false;
	
	
	//////////VCF Files/////////////
	/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
	if(!VCFTableNames.isEmpty()){
		VCFTables = VCFTableNames.split(","); 
	
		if(!VCFCanonicalizeRefNames.isEmpty()){
			VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
			if (VCFCanonicalize.length != VCFTables.length)
				throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
		}
		else{
			System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
		}
	}

		
	/*#######################Prepare VCF queries#######################*/
	String VCFQuery=" ( SELECT * from ";
	String UCSC_VCFQuery=" ( SELECT * from ";

	for (int index=0; index< VCFTables.length; index++){
		if(VCFCanonicalize!=null) {
			if (!GoogleVCF)
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, end_position, reference_bases, alternate_bases.alt as alternate_bases ";
		}else {
			if (!GoogleVCF)
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, end_position , reference_bases, alternate_bases.alt as alternate_bases ";
		}
		//Check UCSC_VCF
		UCSC_VCFQuery = VCFQuery + ", " + build + "_UCSC_knownGene FROM `"+ VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]  +"`) ";
		VCFQuery += " FROM `"+ VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]  +"`, unnest (alternate_bases) as alternate_bases) ";
	}
	VCFQuery +=" ) as VCF ";
	UCSC_VCFQuery +=" ) as VCF ";
	 	 
	String AllFields=""; // Use to 
	String AnnotationQuery="";
	int AnnotationIndex=1; // Use for creating alias names
	
	/*#######################Prepare VCF queries#######################*/
	if(VariantAnnotationTables!=null){
		LOG.info("Variant Annotations ");
		for (int index=0; index< VariantAnnotationTables.length; index++,AnnotationIndex++){
					
					/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
					 * ProjectId: gbsc-gcp-project-cba
					 * DatasetId: PublicAnnotationSets
					 * TableId: hg19_GME
					 * Features: GME_AF:GME_NWA:GME_NEA
					 */
					
					String [] TableInfo = VariantAnnotationTables[index].split(":");
											
					String RequestedFields="";
					String AliasTableName= "Annotation" + AnnotationIndex; 
					String TableName = TableInfo[0]+"."+TableInfo[1];
					
					
					for (int fieldIndex=2; fieldIndex<TableInfo.length; fieldIndex++){
						if (TableInfo.length>3){ // Creating CONCAT
							if (fieldIndex+1 < TableInfo.length) { // This will add "/" after fields => not the last field CONCAT(X, "/", Y, "/", Z) 
								if(createVCF)
									RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" , \"/\" ,";
								else 
									RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`,";
							}
							else
								RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`";
						}
						else //If there is only one feature
							RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`";
					
									
						
						//TODO: fix createVCF
						if(fieldIndex==2 && createVCF){ //Special case where we add an index to the first field 
								if (TableInfo.length>3){ //Top Select 
									//(e.g.,     CONCAT(1: Annotation3.Annotation) 
										AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
								}
								else{//(e.g.,     CONCAT(1: Annotation3.name2) 
										AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
								}
							}
						else if (!createVCF) {// in case of creating Table  
							if (GroupBy) //ARRAY_AGG
								AllFields += ", max(`"+ AliasTableName +"." + TableInfo[fieldIndex] + "`) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex] + "`";
							else	
								AllFields += ", `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` as `" +  TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex] + "`";
						}							
					}
					
					//PADDING: Adjust number of fields
					String Padding="";
					for (int adjustIndex=0; adjustIndex<allAnnotationParams.size(); adjustIndex++) {

						String[] FieldNames = allAnnotationParamsValues.get(adjustIndex);
						
						if(adjustIndex< index ) {
							for (int i=0; i< allAnnotationParams.get(adjustIndex); i++) {
								Padding =  Padding + "'' as `Annotation" + (adjustIndex+1) + "." + FieldNames[i+2] + "`, " ;
							}
						}else if (adjustIndex > index) {
							for (int i=0; i< allAnnotationParams.get(adjustIndex); i++) {
								Padding =  Padding + ",'' as `Annotation" + (adjustIndex+1) + "." + FieldNames[i+2] + "` ";
							}
						}
						else if (adjustIndex==index)
							Padding += RequestedFields;
					}
					RequestedFields=Padding;
					
					//IF the number of fields is more that 1 -> then concat all of them
					if (TableInfo.length>3) {
						
						AnnotationQuery += "  SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases ";
						if (numSamples && GoogleVCF) {
							AnnotationQuery += ", num_samples ";
						}
						
						if(createVCF)
							AnnotationQuery += ", CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
						else 
							AnnotationQuery += ", " + RequestedFields ;

					}
					else {
						AnnotationQuery += " SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, ";
						if (numSamples && GoogleVCF) {
							AnnotationQuery += " num_samples, ";
						}
						AnnotationQuery +=  RequestedFields;
					}
					
					AnnotationQuery +=
//>>>>>>>>>						 " FROM " + VCFQuery 
						 " FROM " + UCSC_VCFQuery
						+ " JOIN `" + TableName +"` AS " + AliasTableName ;
					AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";

					AnnotationQuery += " AND (" + AliasTableName + ".start_position = VCF.start_position) AND (" + AliasTableName + ".end_position = VCF.end_position) "
						+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alternate_bases) = VCF.alternate_bases) "
						+ " OR " + AliasTableName + ".alternate_bases = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".reference_bases)) ";
					
					//This is the case when we have transcript annotations 
					if(index+1 <  VariantAnnotationTables.length || (TranscriptAnnotations!=null))
						AnnotationQuery +=  " UNION ALL ";
					else
						AnnotationQuery +=  " ";
						
		}
	}
	
	 
	if(TranscriptAnnotations!=null){
		for (int index=0; index< TranscriptAnnotations.length; index++, AnnotationIndex++){
			LOG.info("Generic Annotations ");

			/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
			* ProjectId: gbsc-gcp-project-cba
			* DatasetId: PublicAnnotationSets
			* TableId: hg19_refGene_chr17
			* Features: exonCount:exonStarts:exonEnds:score
			*/
			
			String [] TableInfo = TranscriptAnnotations[index].split(":");
							
			String RequestedFields="";
			String AliasTableName= "Annotation" + AnnotationIndex; 
			String TableName = TableInfo[0]+"."+TableInfo[1];
			
			
			for (int fieldIndex=2; fieldIndex<TableInfo.length; fieldIndex++){
				if (TableInfo.length>3){ // Creating CONCAT
					if (fieldIndex+1 < TableInfo.length){
						if(createVCF)
							RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" , \"/\" ,";
						else
							RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`,";
					}
					else
						RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`";
				}
				else //If there is only one feature
					RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`";

							
				
				if(fieldIndex==2 && createVCF){
					int tempIndex=0;
					if(VariantAnnotationTables!=null) {
						tempIndex=VariantAnnotationTables.length;
					}
					
					if (TableInfo.length>3){ //Top Select 
						AllFields += ", CONCAT(\""+ (index+1+tempIndex) +": \","+ AliasTableName +"." + AliasTableName + " ) ";

//						if(VariantAnnotationTables!=null) {
//								AllFields += ", CONCAT(\""+ (index+1+VariantAnnotationTables.length) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
//						}
//						else {
//								AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
//						}
					}
					else{
						AllFields += ", CONCAT(\""+ (index+1+tempIndex) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";

//						if(VariantAnnotationTables!=null) {
//								AllFields += ", CONCAT(\""+ (index+1+VariantAnnotationTables.length) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
//						}
//						else	 {
//								AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";								
//						}
					}
				}	
				else if (!createVCF) {// in case of creating Table  
					if (GroupBy) //ARRAY_AGG
						AllFields += ", max(`"+ AliasTableName +"." + TableInfo[fieldIndex] + "`) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex] + "`";
					else	
						AllFields += ", `"+ AliasTableName +"." + TableInfo[fieldIndex] +"` as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex] + "`";
				}
			}


			
			//PADDING: Adjust number of fields
			int padding_index=allAnnotationParams.size() - TranscriptAnnotations.length + index;
			String Padding="";

			for (int adjustIndex=0; adjustIndex<allAnnotationParams.size(); adjustIndex++) {
				String[] FieldNames = allAnnotationParamsValues.get(adjustIndex);

				if(adjustIndex < padding_index ) {
					for (int i=0; i< allAnnotationParams.get(adjustIndex); i++) {
						Padding =  Padding + "'' as `Annotation" + (adjustIndex+1) + "." + FieldNames[i+2] + "`, " ;
					}
				}else if (adjustIndex > padding_index) {
					for (int i=0; i< allAnnotationParams.get(adjustIndex); i++) {
						Padding =  Padding + ",'' as `Annotation" + (adjustIndex+1) + "." + FieldNames[i+2] + "` ";
					}
				}
				else if (adjustIndex == padding_index)
					Padding+=RequestedFields;
			}
			
			RequestedFields=Padding;

			
			String innerSelect=""; 
			//IF the number of fields is more that 1 -> then concatenation all of them
			if (TableInfo.length>3) {
				innerSelect += " SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, "
				+ "VCF.alternate_bases ";

				if (numSamples && GoogleVCF) {
					innerSelect += ", num_samples ";
				}
				
				if(createVCF)
					innerSelect += ", CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
				else 
					innerSelect += ", " + RequestedFields ;				
					//, CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
			}
			else {
				innerSelect += " SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, ";
				if (numSamples && GoogleVCF) {
					innerSelect += " num_samples, ";
				}	
				innerSelect += RequestedFields;
			}

			if (!LeftJoin) { //Everything
				AnnotationQuery += innerSelect + " FROM " + UCSC_VCFQuery 
						+ " JOIN `" + TableName +"` AS " + AliasTableName ;
				AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
						+ " AND ( DIV(VCF.start_position," + binSize + ")=DIV("+ AliasTableName + ".start_position, "+ binSize + ")) ";
				AnnotationQuery += " WHERE "
					+ " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) "; 
				
				AnnotationQuery += " UNION ALL " 
					+ innerSelect + " FROM " + UCSC_VCFQuery
					+ " JOIN `" + TableName +"` AS " + AliasTableName 	
					+ " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
					+ " AND ( DIV(VCF.end_position," + binSize + ")=DIV("+ AliasTableName +".end_position, "+ binSize + ")) "
				    + " WHERE "
				    + " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) ";

				AnnotationQuery += " UNION ALL " 
						+ innerSelect + " FROM " + UCSC_VCFQuery
						+ " JOIN `" + TableName +"` AS " + AliasTableName 	
						+ " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
						+ " AND ( DIV(VCF.start_position," + binSize + ")=DIV("+ AliasTableName +".end_position, "+ binSize + ")) "
					    + " WHERE "
					    + " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) ";
				
				AnnotationQuery += " UNION ALL " 
						+ innerSelect + " FROM " + UCSC_VCFQuery
						+ " JOIN `" + TableName +"` AS " + AliasTableName 	
						+ " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
						+ " AND ( DIV(VCF.end_position," + binSize + ")=DIV("+ AliasTableName +".start_position, "+ binSize + ")) "
					    + " WHERE "
					    + " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) ";

			}
			else { // VCF annd UCSC_knownGene
				AnnotationQuery +=
						innerSelect + " FROM " + VCFQuery 
						+ " LEFT JOIN `" + TableName +"` AS " + AliasTableName ;
				AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) AND  ("+ 
						AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) ";						

			}
			
			if(index+1 <  TranscriptAnnotations.length || UCSCTranscriptAnnotations!=null)
				AnnotationQuery +=  " UNION ALL ";
			else
				AnnotationQuery +=  " ";
		}
	}
	
	
	if(UCSCTranscriptAnnotations!=null){
		for (int index=0; index< UCSCTranscriptAnnotations.length; index++, AnnotationIndex++){
			LOG.info("UCSC Generic Annotations " + UCSCTranscriptAnnotations[index]);

			/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
			* ProjectId: gbsc-gcp-project-cba
			* DatasetId: PublicAnnotationSets
			* TableId: hg19_refGene_chr17
			* Features: exonCount:exonStarts:exonEnds:score
			*/
			
			String [] TableInfo = UCSCTranscriptAnnotations[index].split(":");
							
			String RequestedFields="";
			String AliasTableName= "Annotation" + AnnotationIndex; 
			String TableName = TableInfo[0]+"."+TableInfo[1];
			
			
			for (int fieldIndex=2; fieldIndex<TableInfo.length; fieldIndex++){
				if (TableInfo.length>3){ // Creating CONCAT
					if (fieldIndex+1 < TableInfo.length){
						if(createVCF)
							RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" , \"/\" ,";
						else
							RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`,";
					}
					else
						RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`";
				}
				else //If there is only one feature
					RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`";

				LOG.info("RequestedFields" + RequestedFields);

							
				if(fieldIndex==2 && createVCF){

					int tempIndex=0;
					if(VariantAnnotationTables!=null) {
						tempIndex+=VariantAnnotationTables.length;
					}
					if (TranscriptAnnotations!=null) {
						tempIndex+=TranscriptAnnotations.length;
					}
					
					
					if (TableInfo.length>3){ //Top Select 
						AllFields += ", CONCAT(\""+ (index+1+tempIndex) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
					}
					else{
						AllFields += ", CONCAT(\""+ (index+1+tempIndex) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
					}
				}	
				
				else if (!createVCF) {// in case of creating Table  

					if (GroupBy) //ARRAY_AGG
						AllFields += ", max(`"+ AliasTableName +"." + TableInfo[fieldIndex] + "`) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex] + "`";
					else	
						AllFields += ", `"+ AliasTableName +"." + TableInfo[fieldIndex] +"` as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex] + "`";
				}

			}


			//PADDING: Adjust number of fields
			int padding_index=allAnnotationParams.size() - UCSCTranscriptAnnotations.length + index;
			String Padding="";

			for (int adjustIndex=0; adjustIndex<allAnnotationParams.size(); adjustIndex++) {
				String[] FieldNames = allAnnotationParamsValues.get(adjustIndex);

				if(adjustIndex < padding_index ) {
					for (int i=0; i< allAnnotationParams.get(adjustIndex); i++) {
						Padding =  Padding + "'' as `Annotation" + (adjustIndex+1) + "." + FieldNames[i+2] + "`, " ;
					}
				}else if (adjustIndex > padding_index) {
					for (int i=0; i< allAnnotationParams.get(adjustIndex); i++) {
						Padding =  Padding + ",'' as `Annotation" + (adjustIndex+1) + "." + FieldNames[i+2] + "` ";
					}
				}
				else if (adjustIndex == padding_index)
					Padding+=RequestedFields;
			}
			
			RequestedFields=Padding;

			
			//IF the number of fields is more that 1 -> then concatenation all of them
			if (TableInfo.length>3) {
				AnnotationQuery += " SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, "
				+ "VCF.alternate_bases ";

				if (numSamples && GoogleVCF) {
					AnnotationQuery += ", num_samples ";
				}
				
				if(createVCF)
					AnnotationQuery += ", CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
				else 
					AnnotationQuery += ", " + RequestedFields ;				
					//, CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
			}
			else {
				AnnotationQuery += " SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, ";
				if (numSamples && GoogleVCF) {
					AnnotationQuery += " num_samples, ";
				}	
				AnnotationQuery += RequestedFields;
			}
			AnnotationQuery +=
				 " FROM " + UCSC_VCFQuery 
				+ " JOIN `" + TableName +"` AS " + AliasTableName ;
			
			AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";
			AnnotationQuery += "AND (" + AliasTableName + ".UCSC_ID = VCF." + build + "_UCSC_knownGene) "
					+ "AND VCF." + build + "_UCSC_knownGene IS NULL " ; //Intergenic Regions
			
			if(index+1 <  UCSCTranscriptAnnotations.length)
				AnnotationQuery +=  " UNION ALL ";
			else
				AnnotationQuery +=  " ";
		}
	}		

	if (numSamples && GoogleVCF) {
		AllFields = ",  num_samples " + AllFields ;
	}
	String Query= "  SELECT "
			+ " reference_name, start_position, `end_position`, reference_bases, "
			+ " alternate_bases " + AllFields 
			+ " FROM (";
	
	Query += AnnotationQuery;
	
	if(!createVCF && GroupBy) {
		Query += " ) GROUP BY  reference_name, start_position, `end_position`, reference_bases, alternate_bases "; 
		if (numSamples) {
			Query += ",  num_samples " ;
		}
	}
	return Query;
}

public static String prepareAnnotateVariantQueryConcatFields_mVCF_StandardSQL_Concat(String VCFTableNames, String VCF_UCSC_TableNames, 
		String VCFCanonicalizeRefNames, String UCSCTranscriptAnnotationTableNames, String TranscriptAnnotationTableNames,
		  String TranscriptCanonicalizeRefNames, String VariantAnnotationTableNames, 
		  String VariantannotationCanonicalizerefNames, 
		  boolean createVCF, boolean TempVCF, boolean GoogleVCF, boolean numSamples, 
		  String build, boolean LeftJoin, int binSize, boolean CONCAT) {


	System.out.println("bin size: " + binSize );
			
			
	String[] UCSCTranscriptAnnotations=null;		
	String[] TranscriptAnnotations=null;
	String[] VariantAnnotationTables=null;
	String[] VCFTables=null;
	String[] VCF_UCSC_Tables=null;
	String[] VCFCanonicalize=null; 
	String[] TranscriptCanonicalize=null;
	String[] VariantannotationCanonicalize=null;

	query.resetVariables();
	ArrayList<Integer> allAnnotationParams = new ArrayList<Integer>();
	ArrayList<String []> allAnnotationParamsValues = new ArrayList<String []>();

	int numAnnotationDatasets=0;
	
	////////////Variant Annotations///////
	if(VariantAnnotationTableNames!=null){
		VariantAnnotationTables = VariantAnnotationTableNames.split(","); 
		numAnnotationDatasets=+ VariantAnnotationTables.length;

		for (int index=0; index<VariantAnnotationTables.length; index++) {
			allAnnotationParams.add(VariantAnnotationTables[index].split(":").length-2);
			allAnnotationParamsValues.add(VariantAnnotationTables[index].split(":"));
		}

		if(!VariantannotationCanonicalizerefNames.isEmpty()){
			VariantannotationCanonicalize = VariantannotationCanonicalizerefNames.split(","); 
			if (VariantannotationCanonicalize.length != VariantAnnotationTables.length)
				throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
		}
	}
	
	/////////////////////Transcripts//////////////
	if(TranscriptAnnotationTableNames!=null){
		TranscriptAnnotations = TranscriptAnnotationTableNames.split(","); 
		numAnnotationDatasets=+ TranscriptAnnotations.length;
		for (int index=0; index<TranscriptAnnotations.length; index++) {			
			allAnnotationParams.add(TranscriptAnnotations[index].split(":").length-2);
			allAnnotationParamsValues.add(TranscriptAnnotations[index].split(":"));
		}
		if(!TranscriptCanonicalizeRefNames.isEmpty()){
			TranscriptCanonicalize = TranscriptCanonicalizeRefNames.split(","); 
			
			if (TranscriptAnnotations.length != TranscriptCanonicalize.length)
				throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and transcript tables");
		}
	}

	/////////////////////UCSCTranscripts//////////////
	if(UCSCTranscriptAnnotationTableNames!=null){
		UCSCTranscriptAnnotations = UCSCTranscriptAnnotationTableNames.split(","); 
		numAnnotationDatasets=+ UCSCTranscriptAnnotations.length;
		for (int index=0; index<UCSCTranscriptAnnotations.length; index++) {			
			allAnnotationParams.add(UCSCTranscriptAnnotations[index].split(":").length-2);
			allAnnotationParamsValues.add(UCSCTranscriptAnnotations[index].split(":"));
		}
	}

//	boolean exonic=true;
//	if(exonic) {
//		numAnnotationDatasets++;
//		allAnnotationParams.add(1);
//		if(build.equalsIgnoreCase("hg19"))
//			allAnnotationParamsValues.add("gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene:Type".split(":"));
//		else if(build.equalsIgnoreCase("hg38"))
//			allAnnotationParamsValues.add("gbsc-gcp-project-cba:PublicAnnotationSets.hg38_refGene:Type".split(":"));	
//	}
	
	//BigQuery Limit
	boolean GroupBy=true;
//	if(numAnnotationDatasets<=64)
//		GroupBy=true;
//	else
//		GroupBy=false;
	
	
	//////////VCF Files/////////////
	/*Check if the VCF table contains any prefix for the reference fields (e.g., chr => chr17 )*/
	if(!VCFTableNames.isEmpty()){
		VCFTables = VCFTableNames.split(","); 
		if(VCF_UCSC_TableNames!="")
			VCF_UCSC_Tables=VCF_UCSC_TableNames.split(","); 
		
		if(!VCFCanonicalizeRefNames.isEmpty()){
			VCFCanonicalize = VCFCanonicalizeRefNames.split(","); 
			if (VCFCanonicalize.length != VCFTables.length)
				throw new IllegalArgumentException("Mismatched between the number of submitted canonicalize parameters and variant annotation tables");
		}
		else{
			System.out.println("#### Warning: the number of submitted parameters for canonicalizing VCF tables is zero! default prefix value for referenceId is ''");
		}
	}

		
	/*#######################Prepare VCF queries#######################*/
	String VCFQuery=" ( SELECT * from ";
	String UCSC_VCFQuery=" ( SELECT * from ";

	for (int index=0; index< VCFTables.length; index++){
		if(VCFCanonicalize!=null) { 
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases  ";
		}else {
			UCSC_VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases ";
			VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start_position, `end_position`, reference_bases, alternate_bases ";
		}
	
		//Check UCSC_VCF
		if (VCF_UCSC_Tables!=null) {
			UCSC_VCFQuery += ", " + build + "_UCSC_knownGene FROM `"+ VCF_UCSC_Tables[index].split(":")[0] + "." + VCF_UCSC_Tables[index].split(":")[1]  +"`) ";

		}
		VCFQuery += " FROM `"+ VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]  +"`) ";

	}
		
	VCFQuery +=" ) as VCF ";
	UCSC_VCFQuery +=" ) as VCF ";

	 	 
	String AllFields=""; // Use to 
	String AnnotationQuery="";
	int AnnotationIndex=1; // Use for creating alias names
	
	/*#######################Prepare Variant Annotation queries#######################*/
	if(VariantAnnotationTables!=null){
		LOG.info("Variant Annotations ");
		for (int index=0; index< VariantAnnotationTables.length; index++,AnnotationIndex++){
					
					/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
					 * ProjectId: gbsc-gcp-project-cba
					 * DatasetId: PublicAnnotationSets
					 * TableId: hg19_GME
					 * Features: GME_AF:GME_NWA:GME_NEA
					 */
					
					String [] TableInfo = VariantAnnotationTables[index].split(":");
											
					//String RequestedFields="";
					String AliasTableName= "Annotation" + AnnotationIndex; 
					String TableName = TableInfo[0]+"."+TableInfo[1];
						
					int tempIndex=0;
					if (TranscriptAnnotations!=null) {
						tempIndex+=TranscriptAnnotations.length;
					}
					if (UCSCTranscriptAnnotations!=null) {
						tempIndex+=UCSCTranscriptAnnotations.length;
					}
//					if(exonic)
//						tempIndex++;
					
					query.buildRequestedFields(TableInfo, AliasTableName, createVCF, false, CONCAT);
					query.padding(allAnnotationParams, allAnnotationParamsValues, VariantAnnotationTables.length+tempIndex, index, CONCAT);

					
					//IF the number of fields is more that 1 -> then concat all of them
					if (TableInfo.length>3) {
						
						AnnotationQuery += "  SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases ";
						if (numSamples && GoogleVCF) {
							AnnotationQuery += ", num_samples ";
						}
						
						if(createVCF)
							AnnotationQuery += ", CONCAT(" + query.getRequestedFields() +") as " + AliasTableName +"." + AliasTableName;
						else 
							AnnotationQuery += ", " + query.getRequestedFields() ;

					}
					else {
						AnnotationQuery += " SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, ";
						if (numSamples && GoogleVCF) {
							AnnotationQuery += " num_samples, ";
						}
						AnnotationQuery +=  query.getRequestedFields();
					}
					
					AnnotationQuery +=
						 " FROM " + VCFQuery
						+ " JOIN `" + TableName +"` AS " + AliasTableName ;
					AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";

					AnnotationQuery += " AND (" + AliasTableName + ".start_position = VCF.start_position) AND (" + AliasTableName + ".end_position = VCF.end_position) "
						+ " WHERE ((("+ AliasTableName + ".reference_bases	= \"\")  "
								+ "AND (CONCAT(VCF.reference_bases, " + AliasTableName + ".alternate_bases) = VCF.alternate_bases) "
						+ " OR " + AliasTableName + ".alternate_bases = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".reference_bases)) ";
					
					//This is the case when we have transcript annotations 
					if(index+1 < VariantAnnotationTables.length) {
						AnnotationQuery +=  " UNION ALL ";
					}
					else
						AnnotationQuery +=  " ";					
		}
	}
	
	//This is the case when we have transcript annotations 
	if( VariantAnnotationTables!=null && (TranscriptAnnotations!=null  || UCSCTranscriptAnnotations!=null))
		AnnotationQuery +=  " UNION ALL ";
	else
		AnnotationQuery +=  " ";
	
	
	if(TranscriptAnnotations!=null){
		for (int index=0; index< TranscriptAnnotations.length; index++, AnnotationIndex++){
			LOG.info("Generic Annotations ");

			/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
			* ProjectId: gbsc-gcp-project-cba
			* DatasetId: PublicAnnotationSets
			* TableId: hg19_refGene_chr17
			* Features: exonCount:exonStarts:exonEnds:score
			*/
			
			String [] TableInfo = TranscriptAnnotations[index].split(":");
			
			//String RequestedFields="";
			String AliasTableName= "Annotation" + AnnotationIndex;
			String TableName = TableInfo[0]+"."+TableInfo[1];
			
			int tempIndex=0;
			if (UCSCTranscriptAnnotations!=null) {
				tempIndex+=UCSCTranscriptAnnotations.length;
			}
//			if(exonic)
//				tempIndex++;
			
			
			query.buildRequestedFields(TableInfo, AliasTableName, createVCF, true, CONCAT);
			query.padding(allAnnotationParams, allAnnotationParamsValues, TranscriptAnnotations.length+tempIndex, index, CONCAT );
			
			String innerSelect=""; 
			//IF the number of fields is more that 1 -> then concatenation all of them
			if (TableInfo.length>3) {
				innerSelect += " SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, "
				+ "VCF.alternate_bases ";

				if (numSamples && GoogleVCF) {
					innerSelect += ", num_samples ";
				}
				
				if(createVCF)
					innerSelect += ", CONCAT(" + query.getRequestedFields() +") as " + AliasTableName +"." + AliasTableName;
				else 
					innerSelect += ", " + query.getRequestedFields() ;				
					//, CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
			}
			else {
				innerSelect += " SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, ";
				if (numSamples && GoogleVCF) {
					innerSelect += " num_samples, ";
				}	
				innerSelect += query.getRequestedFields();
			}

			if (!LeftJoin) { //Everything
				AnnotationQuery += "(" + innerSelect + " FROM " + VCFQuery 
						+ " JOIN `" + TableName +"` AS " + AliasTableName ;
				AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
						+ " AND ( DIV(VCF.start_position," + binSize + ")=DIV("+ AliasTableName + ".start_position, "+ binSize + ")) ";
				AnnotationQuery += " WHERE "
					+ " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) "; 
				
				AnnotationQuery += " UNION DISTINCT " 
					+ innerSelect + " FROM " + VCFQuery
					+ " JOIN `" + TableName +"` AS " + AliasTableName 	
					+ " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
					+ " AND ( DIV(VCF.end_position," + binSize + ")=DIV("+ AliasTableName +".end_position, "+ binSize + ")) "
				    + " WHERE "
				    + " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) ";

				AnnotationQuery += " UNION DISTINCT " 
						+ innerSelect + " FROM " + VCFQuery
						+ " JOIN `" + TableName +"` AS " + AliasTableName 	
						+ " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
						+ " AND ( DIV(VCF.start_position," + binSize + ")=DIV("+ AliasTableName +".end_position, "+ binSize + ")) "
					    + " WHERE "
					    + " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) ";
				
				AnnotationQuery += " UNION DISTINCT " 
						+ innerSelect + " FROM " + VCFQuery
						+ " JOIN `" + TableName +"` AS " + AliasTableName 	
						+ " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
						+ " AND ( DIV(VCF.end_position," + binSize + ")=DIV("+ AliasTableName +".start_position, "+ binSize + ")) "
					    + " WHERE "
					    + " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position )) ";

			}
			else { // VCF annd UCSC_knownGene
//				AnnotationQuery +=
//						innerSelect + " FROM " + VCFQuery 
//						+ " LEFT JOIN `" + TableName +"` AS " + AliasTableName ;
//				AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) AND  ("+ 
//						AliasTableName +".start <= VCF.END) AND (VCF.start <= "+ AliasTableName +".end ) ";	
				
				AnnotationQuery += innerSelect + " FROM " + VCFQuery 
						+ " LEFT JOIN `" + TableName +"` AS " + AliasTableName ;
				AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
						+ " AND ( DIV(VCF.start_position," + binSize + ")=DIV("+ AliasTableName + ".start_position, "+ binSize + ")) ";
				AnnotationQuery += " WHERE "
					+ " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) "; 
				
				AnnotationQuery += " UNION DISTINCT " 
					+ innerSelect + " FROM " + VCFQuery
					+ " LEFT JOIN `" + TableName +"` AS " + AliasTableName 	
					+ " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
					+ " AND ( DIV(VCF.end_position," + binSize + ")=DIV("+ AliasTableName +".end_position, "+ binSize + ")) "
				    + " WHERE "
				    + " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) ";

				AnnotationQuery += " UNION DISTINCT " 
						+ innerSelect + " FROM " + VCFQuery
						+ " LEFT JOIN `" + TableName +"` AS " + AliasTableName 	
						+ " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
						+ " AND ( DIV(VCF.start_position," + binSize + ")=DIV("+ AliasTableName +".end_position, "+ binSize + ")) "
					    + " WHERE "
					    + " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) ";
				
				AnnotationQuery += " UNION DISTINCT " 
						+ innerSelect + " FROM " + VCFQuery
						+ " LEFT JOIN `" + TableName +"` AS " + AliasTableName 	
						+ " ON (" + AliasTableName + ".reference_name = VCF.reference_name) "
						+ " AND ( DIV(VCF.end_position," + binSize + ")=DIV("+ AliasTableName +".start_position, "+ binSize + ")) "
					    + " WHERE "
					    + " ("+ AliasTableName +".start_position <= VCF.end_position) AND (VCF.start_position <= "+ AliasTableName +".end_position ) ";

			}
			
			if(index+1 < TranscriptAnnotations.length)
				AnnotationQuery +=  " UNION ALL ";
			else
				AnnotationQuery +=  " ";
		}
	}
	
	if( TranscriptAnnotations!=null &&  UCSCTranscriptAnnotations!=null)
		AnnotationQuery +=  " UNION ALL ";
	else
		AnnotationQuery +=  " ";
	
	//AllFields=query.getAllFields();	
	
	if(UCSCTranscriptAnnotations!=null){
		for (int index=0; index< UCSCTranscriptAnnotations.length; index++, AnnotationIndex++){
			LOG.info("UCSC Generic Annotations " + UCSCTranscriptAnnotations[index]);

			/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
			* ProjectId: gbsc-gcp-project-cba
			* DatasetId: PublicAnnotationSets
			* TableId: hg19_refGene_chr17
			* Features: exonCount:exonStarts:exonEnds:score
			*/
			
			String [] TableInfo = UCSCTranscriptAnnotations[index].split(":");
							
//			String RequestedFields="";
			String AliasTableName= "Annotation" + AnnotationIndex; 
			String TableName = TableInfo[0]+"."+TableInfo[1];
			
			int tempIndex=0;
//			if(exonic)
//				tempIndex=1;
			
			query.buildRequestedFields(TableInfo, AliasTableName, createVCF, false, CONCAT);
			query.padding(allAnnotationParams, allAnnotationParamsValues, UCSCTranscriptAnnotations.length+tempIndex, index, CONCAT );

			//IF the number of fields is more that 1 -> then concatenation all of them
			if (TableInfo.length>3) {
				AnnotationQuery += " SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, "
				+ "VCF.alternate_bases ";

				if (numSamples && GoogleVCF) {
					AnnotationQuery += ", num_samples ";
				}
				
				if(createVCF)
					AnnotationQuery += ", CONCAT(" + query.getRequestedFields() +") as " + AliasTableName +"." + AliasTableName;
				else 
					AnnotationQuery += ", " + query.getRequestedFields() ;				
					//, CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
			}
			else {
				AnnotationQuery += " SELECT VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, ";
				if (numSamples && GoogleVCF) {
					AnnotationQuery += " num_samples, ";
				}	
				AnnotationQuery += query.getRequestedFields();
			}
			AnnotationQuery +=
				 " FROM " + UCSC_VCFQuery 
				+ " JOIN `" + TableName +"` AS " + AliasTableName ;
			
			AnnotationQuery += " ON (" + AliasTableName + ".reference_name = VCF.reference_name) ";
			AnnotationQuery += "AND (" + AliasTableName + ".UCSC_ID = VCF." + build + "_UCSC_knownGene) "
					+ "AND VCF." + build + "_UCSC_knownGene IS NOT NULL " ; 
			
			if(index+1 <  UCSCTranscriptAnnotations.length)
				AnnotationQuery +=  " UNION ALL ";
			else
				AnnotationQuery +=  " ";
		}
	}
	
/// Finding the type of variants
//	if(exonic)
//		AnnotationQuery+= " UNION ALL " + query.findExonic(build, AnnotationIndex++, allAnnotationParams, allAnnotationParamsValues, CONCAT, 
//				numSamples, GoogleVCF, VCFQuery, 2500000); //largest transcript
	
	
	AllFields=query.getAllFields();	

	if (numSamples && GoogleVCF) {
		AllFields = ",  num_samples " + AllFields ;
	}
	String Query= "  SELECT "
			+ " reference_name, start_position, `end_position`, reference_bases, "
			+ " alternate_bases ";
	
	if (	TranscriptAnnotationTableNames!=null && (TranscriptAnnotationTableNames.equalsIgnoreCase("gbsc-gcp-project-cba:AnnotationHive_hg19.hg19_UCSC_knownGene:name")
			|| TranscriptAnnotationTableNames.equalsIgnoreCase("gbsc-gcp-project-cba:AnnotationHive_hg19.hg38_UCSC_knownGene:name"))) {
			Query = AnnotationQuery.replaceAll("Annotation1.name as `Annotation1.name`", "Annotation1.name as `hg19_UCSC_knownGene`");
	}else {
		
		Query+=AllFields + " FROM (" + AnnotationQuery;
		if(!createVCF && GroupBy) {
			Query += " ) GROUP BY  reference_name, start_position, `end_position`, reference_bases, alternate_bases " + query.getGroupByList(); 
			if (numSamples) {
				Query += ",  num_samples " ;
			}
		}
	}

	return Query;
}

 	public static class Query {
	    String RequestedFields="";
	    String GroupByList="";
	    String AllFields="";
	    boolean createVCF=false;
	    boolean GroupBy=false;
    
	    public void setGroupBy(boolean GB) {
	    		GroupBy= GB;
	    }

	    public String findExonic2(String build, int AnnotationIndex, ArrayList<Integer> allAnnotationParams,
	    		ArrayList<String[]> allAnnotationParamsValues, boolean CONCAT, boolean numSamples, boolean GoogleVCF, String VCFQuery, int binSize) {
	    
	    	String s="SELECT SRC.*, DEST.Final as `Final` FROM       "
	    			+ "`gbsc-gcp-project-cba.cancer.LargeExperiment_1000Geneomes_Public_Final_DBs_t3` as SRC JOIN  "
	    			+ "(SELECT     VCF.reference_name as `reference_name` ,     VCF.start_position,     VCF.end_position,     "
	    			+ "VCF.reference_bases as `reference_bases`,     VCF.alternate_bases as `alternate_bases`,   "
	    			+ "ARRAY_AGG (STRUCT(REPLACE(REPLACE(REPLACE(status, '1', ''), '2', ''), '3', ''))    ORDER BY     "
	    			+ "Status ASC    LIMIT     1)[ OFFSET   (0)]  Final FROM (   SELECT     VCF.reference_name as `reference_name`,     "
	    			+ "VCF.start_position,     VCF.end_position,     VCF.reference_bases as `reference_bases`,    "
	    			+ " VCF.alternate_bases as `alternate_bases`,      CASE       WHEN ((AN.start_position > VCF.end_position) OR ( VCF.start_position > AN.end_position)) "
	    			+ "THEN '3Intergenic'       WHEN ((AN.StartPoint <= VCF.end_position)       AND (VCF.start_position <= AN.EndPoint)) THEN '1Exonic'       "
	    			+ "ELSE '2Intronic'     END AS Status   FROM (     SELECT       REPLACE(reference_name, '', '') AS reference_name,       "
	    			+ "start_position,       `end_position`,       reference_bases,       alternate_bases     FROM       "
	    			+ "`gbsc-gcp-project-cba.cancer.LargeExperiment_1000Geneomes_Public_Final_DBs_t3`) AS VCF   LEFT JOIN (     "
	    			+ "SELECT       reference_name,       start_position,       `end_position`,       CAST(StartPoint AS INT64) StartPoint,      "
	    			+ " CAST(EndPoint AS INT64) EndPoint     FROM       `gbsc-gcp-project-cba.AnnotationHive_hg19.hg19_UCSC_refGene` t,       "
	    			+ "UNNEST(SPLIT( exonStarts )) StartPoint     WITH     OFFSET       pos1     JOIN       UNNEST(SPLIT( exonEnds )) EndPoint     "
	    			+ "WITH     OFFSET       pos2     ON       pos1 = pos2     WHERE       EndPoint<>\"\") AS AN   "
	    			+ "ON     VCF.reference_name = AN.reference_name   AND ( DIV(VCF.start_position,2500000)=DIV(AN.start_position, 2500000))   "
	    			+ "GROUP BY     VCF.reference_name,     VCF.start_position,     VCF.end_position,     VCF.reference_bases,     VCF.alternate_bases,     Status  "
	    			+ " UNION DISTINCT    "
	    			+ "SELECT     VCF.reference_name as `reference_name`,     VCF.start_position,     VCF.end_position ,     "
	    			+ "VCF.reference_bases as `reference_bases`,     VCF.alternate_bases as `alternate_bases`,     "
	    			+ " CASE       WHEN ((AN.start_position > VCF.end_position) OR ( VCF.start_position > AN.end_position)) THEN '3Intergenic'      "
	    			+ " WHEN ((AN.StartPoint <= VCF.end_position)       AND (VCF.start_position <= AN.EndPoint)) THEN '1Exonic'       "
	    			+ "ELSE '2Intronic'     END AS Status   FROM (     SELECT       REPLACE(reference_name, '', '') AS reference_name,       "
	    			+ "start_position,       `end_position`,       reference_bases,       alternate_bases     FROM       "
	    			+ "`gbsc-gcp-project-cba.cancer.LargeExperiment_1000Geneomes_Public_Final_DBs_t3`) AS VCF   "
	    			+ "LEFT JOIN (     SELECT       reference_name,       start_position,       `end_position`,       CAST(StartPoint AS INT64) StartPoint,       "
	    			+ "CAST(EndPoint AS INT64) EndPoint     FROM       `gbsc-gcp-project-cba.AnnotationHive_hg19.hg19_UCSC_refGene` t,       "
	    			+ "UNNEST(SPLIT( exonStarts )) StartPoint     WITH     OFFSET       pos1     JOIN       UNNEST(SPLIT( exonEnds )) EndPoint    "
	    			+ " WITH     OFFSET       pos2     ON       pos1 = pos2     WHERE       EndPoint<>\"\") AS AN   ON     "
	    			+ "VCF.reference_name = AN.reference_name   AND (DIV(VCF.start_position,2500000)=DIV(AN.end_position, 2500000)    )   GROUP BY    "
	    			+ " VCF.reference_name,     VCF.start_position,     VCF.end_position,     VCF.reference_bases,     VCF.alternate_bases,     Status,     "
	    			+ "AN.end_position,     AN.start_position   UNION DISTINCT    SELECT     VCF.reference_name as `reference_name`,     VCF.start_position,     "
	    			+ "VCF.end_position ,     VCF.reference_bases as `reference_bases`,     VCF.alternate_bases as `alternate_bases`,      "
	    			+ " CASE       WHEN ((AN.start_position > VCF.end_position) OR ( VCF.start_position > AN.end_position)) THEN '3Intergenic'       WHEN ((AN.StartPoint <= VCF.end_position)       "
	    			+ "AND (VCF.start_position <= AN.EndPoint)) THEN '1Exonic'       ELSE '2Intronic'     END AS Status   "
	    			+ "FROM (     SELECT       REPLACE(reference_name, '', '') AS reference_name,       start_position,       `end_position`,       reference_bases,"
	    			+ " alternate_bases     FROM       "
	    			+ "`gbsc-gcp-project-cba.cancer.LargeExperiment_1000Geneomes_Public_Final_DBs_t3`"
	    			+ ") AS VCF   LEFT JOIN (     SELECT       reference_name,       start_position,       `end_position`,       name,       name2,       "
	    			+ "CAST(StartPoint AS INT64) StartPoint,       CAST(EndPoint AS INT64) EndPoint     FROM      "
	    			+ " `gbsc-gcp-project-cba.AnnotationHive_hg19.hg19_UCSC_refGene` t,       UNNEST(SPLIT( exonStarts )) StartPoint     "
	    			+ "WITH     OFFSET       pos1     JOIN       UNNEST(SPLIT( exonEnds )) EndPoint     WITH     OFFSET       pos2     ON       "
	    			+ "pos1 = pos2     WHERE       EndPoint<>\"\") AS AN   ON     VCF.reference_name = AN.reference_name   "
	    			+ "AND (DIV(VCF.end_position,2500000)=DIV(AN.start_position, 2500000)   )   GROUP BY     VCF.reference_name,     "
	    			+ "VCF.start_position,     VCF.end_position,     VCF.reference_bases,     alternate_bases,     Status   UNION DISTINCT      "
	    			+ "SELECT     VCF.reference_name as `reference_name`,     VCF.start_position,     VCF.end_position,     "
	    			+ "VCF.reference_bases as `reference_bases`,     VCF.alternate_bases as `alternate_bases`,      CASE      "
	    			+ " WHEN ((AN.start_position > VCF.end_position) OR ( VCF.start_position > AN.end_position)) THEN '3Intergenic'       WHEN ((AN.StartPoint <= VCF.end_position)       "
	    			+ "AND (VCF.start_position <= AN.EndPoint)) THEN '1Exonic'       ELSE '2Intronic'     END AS Status   FROM (     SELECT       "
	    			+ "REPLACE(reference_name, '', '') AS reference_name,       start_position,       `end_position`,       reference_bases,       alternate_bases     "
	    			+ "FROM       `gbsc-gcp-project-cba.cancer.LargeExperiment_1000Geneomes_Public_Final_DBs_t3`) AS VCF   LEFT JOIN (     SELECT       "
	    			+ "reference_name,       start_position,       `end_position`,       CAST(StartPoint AS INT64) StartPoint,       CAST(EndPoint AS INT64) EndPoint     "
	    			+ "FROM       `gbsc-gcp-project-cba.AnnotationHive_hg19.hg19_UCSC_refGene` t,       UNNEST(SPLIT( exonStarts )) StartPoint     WITH     "
	    			+ "OFFSET       pos1     JOIN       UNNEST(SPLIT( exonEnds )) EndPoint     WITH     OFFSET       pos2     ON       pos1 = pos2     "
	    			+ "WHERE       EndPoint<>\"\") AS AN   ON     VCF.reference_name = AN.reference_name   AND ( DIV(VCF.end_position,2500000)=DIV(AN.end_position, 2500000)   )   "
	    			+ "GROUP BY     VCF.reference_name,     VCF.start_position,     VCF.end_position,     VCF.reference_bases,     VCF.alternate_bases,     Status    ) "
	    			+ "as VCF GROUP BY    VCF.reference_name,     VCF.start_position ,     VCF.end_position,     VCF.reference_bases,     VCF.alternate_bases) AS DEST "
	    			+ "ON     DEST.reference_name=SRC.reference_name AND     DEST.start_position=SRC.start_position AND     DEST.end_position=SRC.end_position AND     "
	    			+ "DEST.reference_bases= SRC.reference_bases AND     DEST.alternate_bases= SRC.alternate_bases";
	    	
	    	
	    	return "";
	    }	    
	    
//	    public String findExonic(String build, int AnnotationIndex, ArrayList<Integer> allAnnotationParams,
//	    		ArrayList<String[]> allAnnotationParamsValues, boolean CONCAT, boolean numSamples, boolean GoogleVCF, String VCFQuery, int binSize) {
	    	   public String findExonic(String build, int AnnotationIndex, boolean CONCAT, boolean numSamples, boolean GoogleVCF, 
	    			   String VCFQuery, int binSize, String tempTable) {	
	    		String tempQuery="";
	    		String GeneAnnotationTable;
	    		//TODO: Dynamic system
			if(build.equalsIgnoreCase("hg19"))
				GeneAnnotationTable="gbsc-gcp-project-cba:AnnotationHive_hg19.hg19_UCSC_refGene:Type";
			else // if(build.equalsIgnoreCase("hg38"))
				GeneAnnotationTable = "gbsc-gcp-project-cba:cba:AnnotationHive_hg19.hg38_refGene:Type";
			
			/*#######################Prepare Gene Annotation queries#######################*/
			LOG.info("Gene Annotations ");
							
							/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
							 * ProjectId: gbsc-gcp-project-cba
							 * DatasetId: PublicAnnotationSets
							 * TableId: hg19_GME
							 * Features: GME_AF:GME_NWA:GME_NEA
							 */
							
			String [] TableInfo = GeneAnnotationTable.split(":");
													
			//String RequestedFields="";
			String AliasTableName= "Annotation" + AnnotationIndex; 
			String TableName = TableInfo[0]+"."+TableInfo[1];
								

//			query.buildRequestedFields(TableInfo, AliasTableName, createVCF, false, true);
//			query.padding(allAnnotationParams, allAnnotationParamsValues, 1, 1, CONCAT);


//			tempQuery="SELECT VCF.reference_name , VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases,  ";
//			tempQuery +=  query.getRequestedFields().replaceFirst("'' as `Annotation2`,", "");
			
			tempQuery+="SELECT SRC.*, DEST.Final as `Final` FROM       "
	    			+ "`" + tempTable +"` as SRC JOIN  "
	    			+ "(SELECT     VCF.reference_name as `reference_name` ,     VCF.start_position,     VCF.end_position,     "
	    			+ "VCF.reference_bases as `reference_bases`,     VCF.alternate_bases as `alternate_bases`,   ";

			tempQuery +=" ARRAY_AGG (STRUCT(REPLACE(REPLACE(REPLACE(status, '1', ''), '2', ''), '3', ''))   ORDER BY Status ASC   "
					+ "LIMIT 1)[OFFSET  (0)] Final FROM (  "
					+ "SELECT VCF.reference_name as `reference_name`, "
					+ "VCF.start_position, VCF.end_position , VCF.reference_bases as `reference_bases`, VCF.alternate_bases as `alternate_bases`,  "
					+ "CASE   WHEN ((" + AliasTableName + ".start_position > VCF.end_position) OR ( VCF.start_position > " + AliasTableName + ".end_position)) THEN '3Intergenic'   "
					+ "WHEN ((" + AliasTableName + ".StartPoint <= VCF.end_position)   AND (VCF.start_position <= " + AliasTableName + ".EndPoint)) THEN '1Exonic'   "
					+ "ELSE '2Intronic' END AS Status  FROM "
					
					+ VCFQuery
					
					+ " LEFT JOIN "
					+ "( SELECT   reference_name,   start_position,   `end_position`,   CAST(StartPoint AS INT64) StartPoint,   "
					+ "CAST(EndPoint AS INT64) EndPoint FROM   "
					+ "`" + TableName + "` t,   "
					+ "UNNEST(SPLIT( exonStarts )) StartPoint WITH OFFSET   pos1 JOIN   "
					+ "UNNEST(SPLIT( exonEnds )) EndPoint WITH OFFSET   pos2 ON   pos1 = pos2 "
					+ "WHERE   EndPoint<>\"\") AS " + AliasTableName + "  ON VCF.reference_name = " + AliasTableName + ".reference_name  "
					+ "AND ( DIV(VCF.start_position,"+ binSize +")=DIV(" + AliasTableName + ".start_position, "+ binSize +"))  "
					+ "GROUP BY VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, Status  "
					
					+ " UNION DISTINCT   "
					
					+ "SELECT VCF.reference_name as `reference_name`, VCF.start_position, VCF.end_position, "
					+ "VCF.reference_bases as `reference_bases`, VCF.alternate_bases as `alternate_bases`,  "
					+ "CASE   WHEN ((" + AliasTableName + ".start_position > VCF.end_position) OR ( VCF.start_position > " + AliasTableName + ".end_position)) "
					+ "THEN '3Intergenic'   WHEN ((" + AliasTableName + ".StartPoint <= VCF.end_position)   "
					+ "AND (VCF.start_position <= " + AliasTableName + ".EndPoint)) THEN '1Exonic'   ELSE '2Intronic' END AS Status  FROM "
					
					+ VCFQuery
					
					+ " LEFT JOIN ( SELECT   reference_name,   start_position,   `end_position`,   CAST(StartPoint AS INT64) StartPoint,   "
					+ "CAST(EndPoint AS INT64) EndPoint "
					+ "FROM   `" + TableName + "` t,   UNNEST(SPLIT( exonStarts )) StartPoint WITH OFFSET   "
					+ "pos1 JOIN   UNNEST(SPLIT( exonEnds )) EndPoint WITH OFFSET   pos2 ON   pos1 = pos2 WHERE   "
					+ "EndPoint<>\"\") AS " + AliasTableName + "  ON VCF.reference_name = " + AliasTableName + ".reference_name  AND "
					+ "(DIV(VCF.start_position,"+ binSize +")=DIV(" + AliasTableName + ".end_position, "+ binSize +")  )  GROUP BY VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, "
					+ "VCF.alternate_bases, Status "
					
					+ "UNION DISTINCT   "
					
					+ "SELECT VCF.reference_name as `reference_name`, "
					+ "VCF.start_position, VCF.end_position , VCF.reference_bases as `reference_bases`, VCF.alternate_bases as `alternate_bases`,  "
					+ " CASE   WHEN ((" + AliasTableName + ".start_position > VCF.end_position) OR ( VCF.start_position > " + AliasTableName + ".end_position)) THEN '3Intergenic'   "
					+ "WHEN ((" + AliasTableName + ".StartPoint <= VCF.end_position) AND (VCF.start_position <= " + AliasTableName + ".EndPoint)) THEN '1Exonic'   "
					+ "ELSE '2Intronic' END AS Status  "
					+ "FROM "
					
					+ VCFQuery
					
					+ " LEFT JOIN ( SELECT   reference_name,   start_position,   `end_position`,   name,   name2,   CAST(StartPoint AS INT64) StartPoint,   "
					+ "CAST(EndPoint AS INT64) EndPoint FROM   `" + TableName + "` t,   "
					+ "UNNEST(SPLIT( exonStarts )) StartPoint WITH OFFSET   pos1 JOIN   UNNEST(SPLIT( exonEnds )) EndPoint WITH OFFSET   "
					+ "pos2 ON   pos1 = pos2 WHERE   EndPoint<>\"\") AS " + AliasTableName + " ON VCF.reference_name = " + AliasTableName + ".reference_name  AND "
					+ "(DIV(VCF.end_position,"+ binSize +")=DIV(" + AliasTableName + ".start_position, "+ binSize +")  ) "
					+ " GROUP BY VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, alternate_bases, Status  "
					
					+ "UNION DISTINCT  "
					
					+ "SELECT VCF.reference_name as `reference_name`, VCF.start_position, VCF.end_position, "
					+ "VCF.reference_bases as `reference_bases`, VCF.alternate_bases as `alternate_bases`,  CASE   WHEN ((" + AliasTableName 
					+ ".start_position > VCF.end_position) OR ( VCF.start_position > " + AliasTableName + ".end_position)) THEN '3Intergenic'   "
					+ "WHEN ((" + AliasTableName + ".StartPoint <= VCF.end_position)   AND (VCF.start_position <= " + AliasTableName + ".EndPoint)) "
					+ "THEN '1Exonic'   ELSE '2Intronic' END AS Status  FROM "
					
					+ VCFQuery
					
					+ " LEFT JOIN ( SELECT   reference_name,   start_position,   `end_position`,   CAST(StartPoint AS INT64) StartPoint,   CAST(EndPoint AS INT64) "
					+ "EndPoint FROM   `" + TableName + "` t,   UNNEST(SPLIT( exonStarts )) StartPoint "
					+ "WITH OFFSET   pos1 JOIN   UNNEST(SPLIT( exonEnds )) EndPoint WITH OFFSET   pos2 ON   pos1 = pos2 WHERE   EndPoint<>\"\") "
					+ "AS " + AliasTableName + "  ON VCF.reference_name = " + AliasTableName + ".reference_name  AND "
					+ "( DIV(VCF.end_position,"+ binSize +")=DIV(" + AliasTableName + ".end_position, "+ binSize +")  )  "
					+ "GROUP BY VCF.reference_name, VCF.start_position, VCF.end_position, VCF.reference_bases, VCF.alternate_bases, Status   ) as VCF "
					
					+ "GROUP BY   VCF.reference_name, VCF.start_position , VCF.end_position, VCF.reference_bases, VCF.alternate_bases"		
					+ ") AS DEST "
					+ "ON DEST.reference_name=SRC.reference_name AND DEST.start_position=SRC.start_position AND DEST.end_position=SRC.end_position AND     "
					+ "DEST.reference_bases= SRC.reference_bases AND DEST.alternate_bases= SRC.alternate_bases";
			
			return tempQuery;
		}

	    	   
		public void resetVariables() {
		    RequestedFields="";
		    GroupByList="";
		    AllFields="";
		    createVCF=false;
		    GroupBy=false;
	    }
	    
	    public void padding(ArrayList<Integer> allAnnotationParams, ArrayList<String[]> allAnnotationParamsValues,
				int length, int index, boolean concat) {
	    	 	
	    
	    	//PADDING: Adjust number of fields
	    	
			int padding_index=allAnnotationParams.size() - length + index;
			String Padding="";

			for (int adjustIndex=0; adjustIndex<allAnnotationParams.size(); adjustIndex++) {
				String[] FieldNames = allAnnotationParamsValues.get(adjustIndex);
				if(adjustIndex < padding_index ) {
					if (!concat) {
						for (int i=0; i< allAnnotationParams.get(adjustIndex); i++) {
							if(i+1<allAnnotationParams.get(adjustIndex)) {
								Padding += "'' as `Annotation" + (adjustIndex+1) +"."+ FieldNames[i+2] + "`,";
							}
							else {
//								if (adjustIndex+1 == padding_index)
//									Padding += "'' as `Annotation" + (adjustIndex+1) +"."+ FieldNames[i+2] + "`,";
//								else
									Padding += "'' as `Annotation" + (adjustIndex+1) +"."+ FieldNames[i+2] + "`,";

							}
						}
					}else //concat
						Padding =  Padding + "'' as `Annotation" + (adjustIndex+1) +  "`, " ;
				}else if (adjustIndex > padding_index) {
					if(!concat) {
						for (int i=0; i< allAnnotationParams.get(adjustIndex); i++) {
							if(i+1<allAnnotationParams.get(adjustIndex)) {
								Padding += ",'' as `Annotation" + (adjustIndex+1) +"."+ FieldNames[i+2] + "`";
							}
							else {
								Padding += ",'' as `Annotation" + (adjustIndex+1) +"."+ FieldNames[i+2] + "`";
							}
						}
					} else// (concat)
						Padding =  Padding + ",'' as `Annotation" + (adjustIndex+1) + "` ";
				}
				else if (adjustIndex == padding_index)
					Padding+=RequestedFields;
			}
			
			RequestedFields=Padding;
		}

		public void buildRequestedFields(String[] TableInfo, String AliasTableName, boolean createVCF,
				boolean generic, boolean concat) {
					
	    		//String tempName="";
	    		RequestedFields ="";
			//per annotation table -> populate a sub-query
			for (int fieldIndex=2; fieldIndex<TableInfo.length; fieldIndex++){	
				//1. Inner Query
				if (TableInfo.length>3){ //if there are multiple fields from the same table
					if(RequestedFields == "" && concat) // Creating CONCAT
						RequestedFields = "CONCAT ( ";
					
					if (fieldIndex+1 < TableInfo.length){ // All the fields before the last one
						if(createVCF)
							RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" , \"/\" ,";
						else {
							if (concat)
								RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] + ",'|',";
								//	RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] + ",'\\t',";
							else {
								if (fieldIndex>2)
									RequestedFields += ",";
								
								RequestedFields +=  AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`";	
								if(!generic)
									AllFields += ", MAX( CASE WHEN `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` = '' THEN NULL ELSE `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` END) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex]+ "`";
								else
									AllFields += ", STRING_AGG( CASE WHEN `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` = '' THEN NULL ELSE `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` END) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex]+ "`";									
								//								AllFields += ", max(`"+ AliasTableName +"." + TableInfo[fieldIndex] +"`) as `" + TableInfo[1].split("\\.")[1]  +"_" + TableInfo[fieldIndex]+ "`";								
							}
						}
					}
					else { // This for the last field -> "," field
						if(concat) // Close the concatenation OP 
							RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +") as `" + AliasTableName + "`";
						else {
							RequestedFields += "," + AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`";	
							if(!generic)
								AllFields += ", MAX( CASE WHEN `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` = '' THEN NULL ELSE `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` END) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex]+ "`";
							else
								AllFields += ", STRING_AGG( CASE WHEN `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` = '' THEN NULL ELSE `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` END) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex]+ "`";									
							//							AllFields += ", max(`"+ AliasTableName +"." + TableInfo[fieldIndex] +"`) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex]+ "`";
						}
					}
				}
				else //If there is only one feature
					if(concat)
						//RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName + "`";
						RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`";	
					else {
						RequestedFields += AliasTableName +"." + TableInfo[fieldIndex] +" as `" + AliasTableName +"." + TableInfo[fieldIndex] + "`";	
						//AllFields += ", max(`"+ AliasTableName +"." + TableInfo[fieldIndex] +"`) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex]+ "`";
						//AllFields += ", max( CASE WHEN `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` = '' THEN NULL ELSE `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` END) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex]+ "`";
						if(!generic)
							AllFields += ", MAX( CASE WHEN `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` = '' THEN NULL ELSE `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` END) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex]+ "`";
						else
							AllFields += ", STRING_AGG( CASE WHEN `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` = '' THEN NULL ELSE `"+ AliasTableName +"." + TableInfo[fieldIndex] + "` END) as `" + TableInfo[1].split("\\.")[1] +"_" + TableInfo[fieldIndex]+ "`";									
					}		
			}
	    	
			//For concatenation, we should use STRING_AGG at the top query 
			if (!createVCF && concat) {// in case of creating Table  
				
//				AllFields += ", STRING_AGG( CASE WHEN `"+ AliasTableName + "` = '' THEN NULL ELSE `"+ AliasTableName 
//					+ "` END, '\t') as `" + TableInfo[1].split("\\.")[1] + "`";
				
				if(!generic)
					AllFields += ", MAX( `"+ AliasTableName + "`) as `" + TableInfo[1].split("\\.")[1] + "`";
				else
					AllFields += ", STRING_AGG( CASE WHEN `"+ AliasTableName + "` = '' THEN NULL ELSE `"+ AliasTableName 
					+ "` END, '\t') as `" + TableInfo[1].split("\\.")[1] + "`";
					
//				if (TableInfo.length>3){
////					if(!generic) {
////						//AllFields += ", max(`"+ AliasTableName + "`) as `" + TableInfo[1].split("\\.")[1] + "`";
////						AllFields += ", max( CASE WHEN `"+ AliasTableName + "` = '' THEN NULL ELSE `"+ AliasTableName 
////								+ "` END) as `" + TableInfo[1].split("\\.")[1] + "`";
////						//AllFields += ", STRING_AGG( CASE WHEN `"+ AliasTableName + "` = '' THEN NULL ELSE `"+ AliasTableName 
////						//		+ "` END, '\t') as `" + TableInfo[1].split("\\.")[1] + "`";
////						}
////					else
//						AllFields += ", STRING_AGG( CASE WHEN `"+ AliasTableName + "` = '' THEN NULL ELSE `"+ AliasTableName 
//						+ "` END, '\t') as `" + TableInfo[1].split("\\.")[1] + "`";
//					}
//				else	 {
////					if(!generic) {
////						AllFields += ", max( CASE WHEN `"+ AliasTableName + "` = '' THEN NULL ELSE `"+ AliasTableName 
////							+ "` END) as `" + TableInfo[1].split("\\.")[1] + "`";	
////						}
////					else
//						AllFields += ", STRING_AGG( CASE WHEN `"+ AliasTableName + "` = '' THEN NULL ELSE `"+ AliasTableName 
//						+ "` END, '\t') as `" + TableInfo[1].split("\\.")[1] + "`";
//				}
			}
		}

		public boolean getGroupBy() {
	    		return GroupBy;
	    }
	    
	    public void setRequestedFields(String reqf) {
	    		RequestedFields=reqf;
	    }
	    public void addRequestedFields(String reqf) {
			RequestedFields+=reqf;
	    }
	    public String getRequestedFields() {
    			return RequestedFields;
	    }
	    
	    public void setAllFields(String allFields) {
	    		AllFields=allFields;
	    }
	    public String getAllFields() {
	    		return AllFields;	
	    }
	    public void addAllFields(String allFields) {
    			AllFields+=allFields;
	    }
	        
	    public void setGroupByFields(String groupby) {
	    		GroupByList=groupby;
	    }
	    public void addGroupByFields(String groupby) {
	    		GroupByList+=groupby;
	    }
	    public String getGroupByList() {
			return GroupByList;
	    }

    }
 	
 	
	   public static String findVariantFunctionalityWRTGenes(String build, int AnnotationIndex, boolean CONCAT, boolean numSamples, boolean GoogleVCF, 
			   String VCFQuery, int binSize, String VCFTempTable) {	
		String tempQuery="";
		String GeneAnnotationTable;
		//TODO: Dynamic system
	if(build.equalsIgnoreCase("hg19"))
		GeneAnnotationTable="gbsc-gcp-project-cba:AnnotationHive_hg19.hg19_UCSC_refGene:Type";
	else // if(build.equalsIgnoreCase("hg38"))
		GeneAnnotationTable = "gbsc-gcp-project-cba:cba:AnnotationHive_hg19.hg38_refGene:Type";
	
	/*#######################Prepare Gene Annotation queries#######################*/
	LOG.info("Gene Annotations ");
					
					/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
					 * ProjectId: gbsc-gcp-project-cba
					 * DatasetId: PublicAnnotationSets
					 * TableId: hg19_GME
					 * Features: GME_AF:GME_NWA:GME_NEA
					 */
					
	String [] TableInfo = GeneAnnotationTable.split(":");
											
	//String RequestedFields="";
	String AliasTableName= "Annotation" + AnnotationIndex; 
	String TableName = TableInfo[0]+"."+TableInfo[1];
						

//	query.buildRequestedFields(TableInfo, AliasTableName, createVCF, false, true);
//	query.padding(allAnnotationParams, allAnnotationParamsValues, 1, 1, CONCAT);


//	tempQuery="SELECT VCF.reference_name , VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases,  ";
//	tempQuery +=  query.getRequestedFields().replaceFirst("'' as `Annotation2`,", "");
	
	tempQuery+= " " + 
			"SELECT " + 
			"  SRC.*, " + 
			"  DEST.Final as Functionality " + 
			"  FROM `" + 
			 VCFTempTable + "` as SRC " + // `gbsc-gcp-project-cba.AnnotationHive_hg19.hg19_1000_genomes_phase_3_variants_20150220_processed` AS SRC " + 
			"JOIN ( " + 
			"  SELECT " + 
			"    VCF.reference_name AS `reference_name`, " + 
			"    VCF.start_position, " + 
			"    VCF.end_position, " + 
			"    VCF.reference_bases AS `reference_bases`, " + 
			"    VCF.alternate_bases AS `alternate_bases`, " + 
			"    ARRAY_AGG (STRUCT<Status STRING>(SUBSTR(status, 2)) " + 
			"    ORDER BY " + 
			"      Status DESC " + 
			"    LIMIT " + 
			"      1)[ " + 
			"  OFFSET " + 
			"    (0)] Final " + 
			"  FROM ( " + 
			"    SELECT " + 
			"      VCF.reference_name AS `reference_name`, " + 
			"      VCF.start_position, " + 
			"      VCF.end_position, " + 
			"      VCF.reference_bases AS `reference_bases`, " + 
			"      VCF.alternate_bases AS `alternate_bases`, " + 
			"      CASE " + 
			"              " + 
			"        WHEN ((Annotation1.StartPoint <= VCF.end_position) AND (VCF.start_position <= Annotation1.EndPoint))  " + 
			"        THEN (CASE WHEN cdsStartStat!=\"unk\"  " + 
			"        THEN CONCAT('9Exonic',\" - \", name2) ELSE CONCAT('9ncRNA_Exonic',\" - \", name2) END) " + 
			"         " + 
			"        WHEN ( cdsStartStat!=\"unk\" AND (Annotation1.start_position <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.cdsStart1)) THEN CONCAT('8UTR5',\" - \", name2) " + 
			"         " + 
			"        WHEN (cdsStartStat!=\"unk\" AND (Annotation1.cdsEnd1 <= VCF.end_position)  " + 
			"        AND (VCF.start_position <= Annotation1.end_position)) THEN CONCAT('8UTR3',\" - \", name2) " + 
			"         " + 
			"        WHEN ( cdsStartStat!=\"unk\" AND (Annotation1.cdsStart1 <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.cdsEnd1)) THEN " + 
			"        (CASE WHEN (abs (Annotation1.StartPoint - VCF.end_position)<=2) OR  " + 
			"        (abs (VCF.start_position + 1 - Annotation1.EndPoint) <=2) OR (abs (Annotation1.StartPoint  - VCF.start_position + 1)<=2) OR  " + 
			"        (abs (VCF.end_position - Annotation1.EndPoint) <=2) THEN CONCAT('9Splicing',\" - \", name2) " + 
			"        ELSE CONCAT('7Intronic',\" - \", name2) END) " + 
			"         " + 
			"        WHEN ( cdsStartStat=\"unk\" AND (Annotation1.start_position <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.cdsEnd1)) THEN  CONCAT('7ncRNAIntronic',\" - \", name2) " + 
			"         " + 
			"        WHEN (Annotation1.end_position < VCF.end_position AND ((VCF.end_position - Annotation1.end_position) <= 1000)  " + 
			"        AND ((VCF.end_position - Annotation1.end_position) >=0)) OR (Annotation1.start_position > VCF.start_position  " + 
			"        AND ((Annotation1.start_position - VCF.start_position) <= 1000) AND ((Annotation1.start_position - VCF.start_position) >= 0)) THEN (CASE " + 
			"          WHEN ((Annotation1.start_position - VCF.start_position)>= (VCF.end_position - Annotation1.end_position)) THEN CONCAT('4downstream',\" - \", name2) " + 
			"          ELSE concat ('4upstream', " + 
			"          \" - \", " + 
			"          name2) END) " + 
			"        WHEN ((Annotation1.start_position > VCF.end_position) OR ( VCF.start_position > Annotation1.end_position)) THEN '2Intergenic' " + 
			"         " + 
			"      END AS Status " + 
			"    FROM " +
			VCFQuery +
			"    LEFT JOIN ( " + 
			"      SELECT " + 
			"        reference_name, " + 
			"        start_position, " + 
			"        CAST (cdsStart AS INT64) AS cdsStart1, " + 
			"        CAST(cdsEnd AS INT64) AS cdsEnd1, " + 
			"        `end_position`, " + 
			"        CAST(StartPoint AS INT64) StartPoint, " + 
			"        CAST(EndPoint AS INT64) EndPoint, " + 
			"        cdsStartStat, " + 
			"        name2 " + 
			"      FROM " + 
			"        `gbsc-gcp-project-cba.AnnotationHive_hg19.hg19_UCSC_refGene` t, " + 
			"        UNNEST(SPLIT( exonStarts )) StartPoint " + 
			"      WITH " + 
			"      OFFSET " + 
			"        pos1 " + 
			"      JOIN " + 
			"        UNNEST(SPLIT( exonEnds )) EndPoint " + 
			"      WITH " + 
			"      OFFSET " + 
			"        pos2 " + 
			"      ON " + 
			"        pos1 = pos2 " + 
			"      WHERE " + 
			"        EndPoint<>\"\") AS Annotation1 " + 
			"    ON " + 
			"      VCF.reference_name = Annotation1.reference_name " + 
			"      AND ( DIV(VCF.start_position, "+ binSize +")=DIV(Annotation1.start_position, "+ binSize +")) " + 
			"    GROUP BY " + 
			"      VCF.reference_name, " + 
			"      VCF.start_position, " + 
			"      VCF.end_position, " + 
			"      VCF.reference_bases, " + 
			"      VCF.alternate_bases, " + 
			"      Status " + 
			"    UNION DISTINCT " + 
			"    SELECT " + 
			"      VCF.reference_name AS `reference_name`, " + 
			"      VCF.start_position, " + 
			"      VCF.end_position, " + 
			"      VCF.reference_bases AS `reference_bases`, " + 
			"      VCF.alternate_bases AS `alternate_bases`, " + 
			"      CASE " + 
			"   WHEN ((Annotation1.StartPoint <= VCF.end_position) AND (VCF.start_position <= Annotation1.EndPoint)) THEN (CASE WHEN cdsStartStat!=\"unk\"  " + 
			"        THEN CONCAT('9Exonic',\" - \", name2) ELSE CONCAT('9ncRNA_Exonic',\" - \", name2) END) " + 
			"        WHEN ( cdsStartStat!=\"unk\" AND (Annotation1.start_position <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.cdsStart1)) THEN CONCAT('8UTR5',\" - \", name2) " + 
			"        WHEN (cdsStartStat!=\"unk\" AND (Annotation1.cdsEnd1 <= VCF.end_position)  " + 
			"        AND (VCF.start_position <= Annotation1.end_position)) THEN CONCAT('8UTR3',\" - \", name2) " + 
			"        WHEN ( cdsStartStat!=\"unk\" AND (Annotation1.cdsStart1 <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.cdsEnd1)) THEN " + 
			"        (CASE WHEN (abs (Annotation1.StartPoint - VCF.end_position)<=2) OR  " + 
			"        (abs (VCF.start_position + 1 - Annotation1.EndPoint) <=2) OR (abs (Annotation1.StartPoint  - VCF.start_position + 1)<=2) OR  " + 
			"        (abs (VCF.end_position - Annotation1.EndPoint) <=2) THEN CONCAT('9Splicing',\" - \", name2) " + 
			"        ELSE CONCAT('7Intronic',\" - \", name2) END) " + 
			"        WHEN ( cdsStartStat=\"unk\" AND (Annotation1.start_position <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.cdsEnd1)) THEN  CONCAT('7ncRNAIntronic',\" - \", name2) " + 
			"        WHEN (Annotation1.end_position < VCF.end_position AND ((VCF.end_position - Annotation1.end_position) <= 1000)  " + 
			"        AND ((VCF.end_position - Annotation1.end_position) >=0)) OR (Annotation1.start_position > VCF.start_position  " + 
			"        AND ((Annotation1.start_position - VCF.start_position) <= 1000) AND ((Annotation1.start_position - VCF.start_position) >= 0)) THEN (CASE " + 
			"          WHEN ((Annotation1.start_position - VCF.start_position)>= (VCF.end_position - Annotation1.end_position)) THEN CONCAT('4downstream',\" - \", name2) " + 
			"          ELSE concat ('4upstream', " + 
			"          \" - \", " + 
			"          name2) END) " + 
			"        WHEN ((Annotation1.start_position > VCF.end_position) OR ( VCF.start_position > Annotation1.end_position)) THEN '2Intergenic' " + 
			"         " + 
			"      END AS Status " + 
			"    FROM "+
			VCFQuery
			+
			"    LEFT JOIN ( " + 
			"      SELECT " + 
			"        reference_name, " + 
			"        start_position, " + 
			"        CAST (cdsStart AS INT64) AS cdsStart1, " + 
			"        CAST(cdsEnd AS INT64) AS cdsEnd1, " + 
			"        `end_position`, " + 
			"        CAST(StartPoint AS INT64) StartPoint, " + 
			"        CAST(EndPoint AS INT64) EndPoint, " + 
			"        name2, " + 
			"        cdsStartStat " + 
			"      FROM " + 
			"       `" + TableName + "` t, " + // `gbsc-gcp-project-cba.AnnotationHive_hg19.hg19_UCSC_refGene` t, " 
			"        UNNEST(SPLIT( exonStarts )) StartPoint " + 
			"      WITH " + 
			"      OFFSET " + 
			"        pos1 " + 
			"      JOIN " + 
			"        UNNEST(SPLIT( exonEnds )) EndPoint " + 
			"      WITH " + 
			"      OFFSET " + 
			"        pos2 " + 
			"      ON " + 
			"        pos1 = pos2 " + 
			"      WHERE " + 
			"        EndPoint<>\"\") AS Annotation1 " + 
			"    ON " + 
			"      VCF.reference_name = Annotation1.reference_name " + 
			"      AND (DIV(VCF.start_position, "+ binSize +")=DIV(Annotation1.end_position, "+ binSize +") ) " + 
			"    GROUP BY " + 
			"      VCF.reference_name, " + 
			"      VCF.start_position, " + 
			"      VCF.end_position, " + 
			"      VCF.reference_bases, " + 
			"      VCF.alternate_bases, " + 
			"      Status " + 
			"    UNION DISTINCT " + 
			"    SELECT " + 
			"      VCF.reference_name AS `reference_name`, " + 
			"      VCF.start_position, " + 
			"      VCF.end_position, " + 
			"      VCF.reference_bases AS `reference_bases`, " + 
			"      VCF.alternate_bases AS `alternate_bases`, " + 
			"      CASE " + 
			"   WHEN ((Annotation1.StartPoint <= VCF.end_position) AND (VCF.start_position <= Annotation1.EndPoint)) THEN (CASE WHEN cdsStartStat!=\"unk\" " + 
			"        THEN CONCAT('9Exonic',\" - \", name2) ELSE CONCAT('9ncRNA_Exonic',\" - \", name2) END)" + 
			"        WHEN ( cdsStartStat!=\"unk\" AND (Annotation1.start_position <= VCF.end_position)" + 
			"        AND (VCF.start_position <= Annotation1.cdsStart1)) THEN CONCAT('8UTR5',\" - \", name2)" + 
			"        WHEN (cdsStartStat!=\"unk\" AND (Annotation1.cdsEnd1 <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.end_position)) THEN CONCAT('8UTR3',\" - \", name2) " + 
			"        WHEN ( cdsStartStat!=\"unk\" AND (Annotation1.cdsStart1 <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.cdsEnd1)) THEN " + 
			"        (CASE WHEN (abs (Annotation1.StartPoint - VCF.end_position)<=2) OR " + 
			"        (abs (VCF.start_position + 1 - Annotation1.EndPoint) <=2) OR (abs (Annotation1.StartPoint  - VCF.start_position + 1)<=2) OR " + 
			"        (abs (VCF.end_position - Annotation1.EndPoint) <=2) THEN CONCAT('9Splicing',\" - \", name2) " + 
			"        ELSE CONCAT('7Intronic',\" - \", name2) END) " + 
			"        WHEN ( cdsStartStat=\"unk\" AND (Annotation1.start_position <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.cdsEnd1)) THEN  CONCAT('7ncRNAIntronic',\" - \", name2) " + 
			"        WHEN (Annotation1.end_position < VCF.end_position AND ((VCF.end_position - Annotation1.end_position) <= 1000)  " + 
			"        AND ((VCF.end_position - Annotation1.end_position) >=0)) OR (Annotation1.start_position > VCF.start_position  " + 
			"        AND ((Annotation1.start_position - VCF.start_position) <= 1000) AND ((Annotation1.start_position - VCF.start_position) >= 0)) THEN (CASE " + 
			"          WHEN ((Annotation1.start_position - VCF.start_position)>= (VCF.end_position - Annotation1.end_position)) THEN CONCAT('4downstream',\" - \", name2) " + 
			"          ELSE concat ('4upstream', " + 
			"          \" - \", " + 
			"          name2) END) " + 
			"        WHEN ((Annotation1.start_position > VCF.end_position) OR ( VCF.start_position > Annotation1.end_position)) THEN '2Intergenic' " + 
			"      END AS Status " + 
			"    FROM " + 
			VCFQuery
			+
			"    LEFT JOIN ( " + 
			"      SELECT " + 
			"        reference_name, " + 
			"        start_position, " + 
			"        `end_position`, " + 
			"        CAST (cdsStart AS INT64) AS cdsStart1, " + 
			"        CAST(cdsEnd AS INT64) AS cdsEnd1, " + 
			"        CAST(StartPoint AS INT64) StartPoint, " + 
			"        CAST(EndPoint AS INT64) EndPoint, " + 
			"        name2, " + 
			"        cdsStartStat " + 
			"      FROM " + 
			"       `" + TableName + "` t, " + // "        `gbsc-gcp-project-cba.AnnotationHive_hg19.hg19_UCSC_refGene` t, " + 
			"        UNNEST(SPLIT( exonStarts )) StartPoint " + 
			"      WITH " + 
			"      OFFSET " + 
			"        pos1 " + 
			"      JOIN " + 
			"        UNNEST(SPLIT( exonEnds )) EndPoint " + 
			"      WITH " + 
			"      OFFSET " + 
			"        pos2 " + 
			"      ON " + 
			"        pos1 = pos2 " + 
			"      WHERE " + 
			"        EndPoint<>\"\") AS Annotation1 " + 
			"    ON " + 
			"      VCF.reference_name = Annotation1.reference_name " + 
			"      AND (DIV(VCF.end_position, "+ binSize +")=DIV(Annotation1.start_position, "+ binSize +") ) " + 
			"    GROUP BY " + 
			"      VCF.reference_name, " + 
			"      VCF.start_position, " + 
			"      VCF.end_position, " + 
			"      VCF.reference_bases, " + 
			"      alternate_bases, " + 
			"      Status " + 
			"    UNION DISTINCT " + 
			"    SELECT " + 
			"      VCF.reference_name AS `reference_name`, " + 
			"      VCF.start_position, " + 
			"      VCF.end_position, " + 
			"      VCF.reference_bases AS `reference_bases`, " + 
			"      VCF.alternate_bases AS `alternate_bases`, " + 
			"      CASE " + 
			"   WHEN ((Annotation1.StartPoint <= VCF.end_position) AND (VCF.start_position <= Annotation1.EndPoint)) THEN (CASE WHEN cdsStartStat!=\"unk\" " + 
			"        THEN CONCAT('9Exonic',\" - \", name2) ELSE CONCAT('9ncRNA_Exonic',\" - \", name2) END) " + 
			"        WHEN ( cdsStartStat!=\"unk\" AND (Annotation1.start_position <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.cdsStart1)) THEN CONCAT('8UTR5',\" - \", name2) " + 
			"        WHEN (cdsStartStat!=\"unk\" AND (Annotation1.cdsEnd1 <= VCF.end_position)  " + 
			"        AND (VCF.start_position <= Annotation1.end_position)) THEN CONCAT('8UTR3',\" - \", name2) " + 
			"        WHEN ( cdsStartStat!=\"unk\" AND (Annotation1.cdsStart1 <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.cdsEnd1)) THEN " + 
			"        (CASE WHEN (abs (Annotation1.StartPoint - VCF.end_position)<=2) OR  " + 
			"        (abs (VCF.start_position + 1 - Annotation1.EndPoint) <=2) OR (abs (Annotation1.StartPoint  - VCF.start_position + 1)<=2) OR  " + 
			"        (abs (VCF.end_position - Annotation1.EndPoint) <=2) THEN CONCAT('9Splicing',\" - \", name2) " + 
			"        ELSE CONCAT('7Intronic',\" - \", name2) END) " + 
			"        WHEN ( cdsStartStat=\"unk\" AND (Annotation1.start_position <= VCF.end_position) " + 
			"        AND (VCF.start_position <= Annotation1.cdsEnd1)) THEN  CONCAT('7ncRNAIntronic',\" - \", name2) " + 
			"         " + 
			"        WHEN (Annotation1.end_position < VCF.end_position AND ((VCF.end_position - Annotation1.end_position) <= 1000)  " + 
			"        AND ((VCF.end_position - Annotation1.end_position) >=0)) OR (Annotation1.start_position > VCF.start_position  " + 
			"        AND ((Annotation1.start_position - VCF.start_position) <= 1000) AND ((Annotation1.start_position - VCF.start_position) >= 0)) THEN (CASE " + 
			"          WHEN ((Annotation1.start_position - VCF.start_position)>= (VCF.end_position - Annotation1.end_position)) THEN CONCAT('4downstream',\" - \", name2) " + 
			"          ELSE concat ('4upstream', " + 
			"          \" - \", " + 
			"          name2) END) " + 
			"        WHEN ((Annotation1.start_position > VCF.end_position) OR ( VCF.start_position > Annotation1.end_position)) THEN '2Intergenic' " + 
			"      END AS Status " + 
			"    FROM " + 
			VCFQuery
			+
			"    LEFT JOIN ( " + 
			"      SELECT " + 
			"        reference_name, " + 
			"        start_position, " + 
			"        `end_position`, " + 
			"        CAST (cdsStart AS INT64) AS cdsStart1, " + 
			"        CAST(cdsEnd AS INT64) AS cdsEnd1, " + 
			"        CAST(StartPoint AS INT64) StartPoint, " + 
			"        CAST(EndPoint AS INT64) EndPoint, " + 
			"        name2, " + 
			"        cdsStartStat " + 
			"      FROM " + 
			"       `" + TableName + "` t, " + // "        `gbsc-gcp-project-cba.AnnotationHive_hg19.hg19_UCSC_refGene` t, " + 
			"        UNNEST(SPLIT( exonStarts )) StartPoint " + 
			"      WITH " + 
			"      OFFSET " + 
			"        pos1 " + 
			"      JOIN " + 
			"        UNNEST(SPLIT( exonEnds )) EndPoint " + 
			"      WITH " + 
			"      OFFSET " + 
			"        pos2 " + 
			"      ON " + 
			"        pos1 = pos2 " + 
			"      WHERE " + 
			"        EndPoint<>\"\") AS Annotation1 " + 
			"    ON " + 
			"      VCF.reference_name = Annotation1.reference_name " + 
			"      AND ( DIV(VCF.end_position, "+ binSize +")=DIV(Annotation1.end_position, "+ binSize +") ) " + 
			"    GROUP BY " + 
			"      VCF.reference_name, " + 
			"      VCF.start_position, " + 
			"      VCF.end_position, " + 
			"      VCF.reference_bases," + 
			"      VCF.alternate_bases, " + 
			"      Status ) AS VCF " + 
			"  GROUP BY " + 
			"    VCF.reference_name," + 
			"    VCF.start_position," + 
			"    VCF.end_position," + 
			"    VCF.reference_bases," + 
			"    VCF.alternate_bases) AS DEST" + 
			" ON" + 
			"  DEST.reference_name=SRC.reference_name" + 
			"  AND DEST.start_position=SRC.start_position" + 
			"  AND DEST.end_position=SRC.end_position" + 
			"  AND DEST.reference_bases= SRC.reference_bases" + 
			"  AND DEST.alternate_bases= SRC.alternate_bases"  ;
		
	return tempQuery;
}
	   
	   
	   public static String findExonicVariantType(String build, int AnnotationIndex, boolean CONCAT, boolean numSamples, boolean GoogleVCF, 
			   String VCFQuery, int binSize, String VCFTempTable) {	
		String tempQuery="";
		String GeneAnnotationTable;
		//TODO: Dynamic system
	if(build.equalsIgnoreCase("hg19"))
		GeneAnnotationTable="gbsc-gcp-project-cba:AnnotationHive_hg19.hg19_UCSC_refGene:Type";
	else // if(build.equalsIgnoreCase("hg38"))
		GeneAnnotationTable = "gbsc-gcp-project-cba:cba:AnnotationHive_hg19.hg38_refGene:Type";
	
	/*#######################Prepare Gene Annotation queries#######################*/
	LOG.info("Gene Annotations ");
					
					/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF:GME_NWA:GME_NEA
					 * ProjectId: gbsc-gcp-project-cba
					 * DatasetId: PublicAnnotationSets
					 * TableId: hg19_GME
					 * Features: GME_AF:GME_NWA:GME_NEA
					 */
					
	String [] TableInfo = GeneAnnotationTable.split(":");
											
	//String RequestedFields="";
	String AliasTableName= "Annotation" + AnnotationIndex; 
	String TableName = TableInfo[0]+"."+TableInfo[1];
						
	
	tempQuery+= 
	"SELECT " + 
	"  SRC.*, " + 
	"  DEST.Type as ExonicType, " +
	"  DEST.Gene_Name as Gene_Name " +
	"  FROM `" + 
			 VCFTempTable + "` as SRC " +  
	"LEFT JOIN ( " +		
			
			
	"SELECT  " + 
	"  reference_name AS `reference_name`, " + 
	"  start_position, " + 
	"  end_position, " + 
	"  reference_bases AS `reference_bases`, " + 
	"  alternate_bases AS `alternate_bases`, " + 
	"  split(Final.TypeGene,'\\t') [SAFE_OFFSET(0)] as `Type`, " + 
	"  split(Final.TypeGene,'\\t') [SAFE_OFFSET(1)] as `Gene_Name`, " + 
	"    codon, " + 
	"  NewCodon " + 
	" " + 
	"  FROM ( " + 
	"SELECT " + 
	"  reference_name AS `reference_name`, " + 
	"  start_position, " + 
	"  end_position, " + 
	"  reference_bases AS `reference_bases`, " + 
	"  alternate_bases AS `alternate_bases`, " + 
	"    codon, " + 
	"    NewCODON, " + 
	"  ARRAY_AGG (STRUCT<TypeGene STRING>(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(syn, 'A', ''), 'B', ''), 'C', ''), 'D', ''), 'E', ''), 'F', ''), 'G', ''), 'H', ''), 'I', ''), 'J', ''), 'K', '')) " + 
	"  ORDER BY " + 
	"    syn ASC " + 
	"  LIMIT " + 
	"    1)[ " + 
	"OFFSET " + 
	"  (0)] Final " + 
	"FROM ( " + 
	"  SELECT " + 
	" " + 
	"    CASE " + 
	" " + 
	"      WHEN (LENGTH(reference_bases)=0) AND ((CODON!=\"TAA\" AND CODON!=\"TAG\" AND CODON!=\"TGA\") AND (LENGTH(SUBSTR(NewCODON,-3))=3 AND ( REGEXP_EXTRACT(NewCODON,\"TAA\") is not NULL OR  REGEXP_EXTRACT(NewCODON,\"TAG\") is not NULL OR  REGEXP_EXTRACT(NewCODON,\"TGA\") is not NULL))) THEN concat(\"Astopgain\", \"\\t\", gene_name2) " + 
	"     " + 
	"      WHEN ( LENGTH(alternate_bases) != LENGTH(reference_bases) AND (MOD(LENGTH(alternate_bases),3) != 0 OR (LENGTH(alternate_bases)=0 AND MOD(LENGTH(reference_bases),3) != 0))) THEN CASE " + 
	"      WHEN LENGTH(alternate_bases) > LENGTH(reference_bases) THEN concat (\"Aframeshift insertion\", \"\\t\", gene_name2) " + 
	"    ELSE concat ( \"Bframeshift deletion\", \"\\t\", gene_name2) " + 
	"  END " + 
	"      WHEN (LENGTH(alternate_bases)>1 OR LENGTH(reference_bases)> 1) AND (LENGTH(alternate_bases)=LENGTH(reference_bases) AND MOD(LENGTH(alternate_bases), 3)!=0) THEN \"Cframeshift block substitution\" " + 
	"      WHEN (ABS(LENGTH(alternate_bases)-LENGTH(reference_bases))<=1 " + 
	"      AND (LENGTH(alternate_bases)=1 " + 
	"        OR LENGTH(reference_bases)=1 )) THEN " + 
	"    CASE " + 
	"      WHEN ((CODON!=\"TAA\" AND CODON!=\"TAG\" AND CODON!=\"TGA\") AND (NewCODON=\"TAA\" OR NewCODON=\"TAG\" OR NewCODON=\"TGA\")) THEN concat(\"Dstopgain\", \"\\t\", gene_name2) " + 
	"      WHEN ((CODON=\"TAA\" " + 
	"        OR CODON=\"TAG\" " + 
	"        OR CODON=\"TGA\") " + 
	"      AND (NewCODON!=\"TAA\" " + 
	"        AND NewCODON!=\"TAG\" " + 
	"        AND NewCODON!=\"TGA\")) THEN concat(\"Estoploss\", \"\\t\", gene_name2) " + 
	"      WHEN ABS(LENGTH(alternate_bases)-LENGTH(reference_bases))=0 AND (LENGTH(alternate_bases)=1) THEN CASE " + 
	"      WHEN end_codon LIKE concat ('%', " + 
	"      NewCodon, " + 
	"      '%') THEN concat(\"Jsynonymous SNV\", \"\\t\", gene_name2) " + 
	"    ELSE " + 
	"    concat(\"Inonsynonymous SNV\", \"\\t\", gene_name2) " + 
	"  END " + 
	"  END " + 
	"      WHEN (MOD(LENGTH(alternate_bases),3) = 0)  THEN CASE " + 
	"      WHEN (LENGTH(alternate_bases)) > 0 THEN concat(\"Fnonframeshift insertion\", \"\\t\", gene_name2) " + 
	"    ELSE " + 
	"    concat(\"Gnonframeshift deletion\", \"\\t\", gene_name2) " + 
	"  END " + 
	"      WHEN (MOD(LENGTH(alternate_bases),3) = 0) AND (LENGTH(alternate_bases)=LENGTH(reference_bases)) THEN concat(\"Hnonframeshift block substitution\", \"\\t\", gene_name2) " + 
	"    ELSE " + 
	"    concat(\"Kunknown\", \"\\t\", gene_name2) " + 
	"  END " + 
	"    AS syn, " + 
	"    reference_name, " + 
	"    start_position, " + 
	"    end_position, " + 
	"    reference_bases, " + 
	"    alternate_bases,  " + 
	"    codon, " + 
	"    NewCODON " + 
	"  FROM ( " + 
	"    SELECT " + 
	"      SRC.*, " + 
	"      DEST.codon_pos, " + 
	"      DEST.codon, " + 
	"      DEST.gene_name2, " + 
	"      DEST.strand, " + 
	"       " + 
	"      CASE   WHEN (LENGTH(SRC.alternate_bases)=1 AND LENGTH(SRC.reference_bases)=1) THEN " + 
	"       " + 
	"            CASE " + 
	"              WHEN DEST.strand=\"-\" THEN CASE " + 
	"              WHEN SRC.alternate_bases=\"A\" THEN CASE " + 
	"              WHEN DEST.codon_pos=\"1\" THEN CONCAT(\"T\", SUBSTR(DEST.codon, 2,3)) " + 
	"              WHEN DEST.codon_pos=\"2\" THEN CONCAT(SUBSTR(DEST.codon, 1,1), \"T\", SUBSTR(DEST.codon, 3,3)) " + 
	"              WHEN DEST.codon_pos=\"3\" THEN CONCAT(SUBSTR(DEST.codon, 1,2), \"T\") " + 
	"          END " + 
	"              WHEN SRC.alternate_bases=\"T\" THEN CASE " + 
	"              WHEN DEST.codon_pos=\"1\" THEN CONCAT(\"A\", SUBSTR(DEST.codon, 2,3)) " + 
	"              WHEN DEST.codon_pos=\"2\" THEN CONCAT(SUBSTR(DEST.codon, 1,1), \"A\", SUBSTR(DEST.codon, 3,3)) " + 
	"              WHEN DEST.codon_pos=\"3\" THEN CONCAT(SUBSTR(DEST.codon, 1,2), \"A\") " + 
	"          END " + 
	"              WHEN SRC.alternate_bases=\"C\" THEN CASE " + 
	"              WHEN DEST.codon_pos=\"1\" THEN CONCAT(\"G\", SUBSTR(DEST.codon, 2,3)) " + 
	"              WHEN DEST.codon_pos=\"2\" THEN CONCAT(SUBSTR(DEST.codon, 1,1), \"G\", SUBSTR(DEST.codon, 3,3)) " + 
	"              WHEN DEST.codon_pos=\"3\" THEN CONCAT(SUBSTR(DEST.codon, 1,2), \"G\") " + 
	"          END " + 
	"              WHEN SRC.alternate_bases=\"G\" THEN CASE " + 
	"              WHEN DEST.codon_pos=\"1\" THEN CONCAT(\"C\", SUBSTR(DEST.codon, 2,3)) " + 
	"              WHEN DEST.codon_pos=\"2\" THEN CONCAT(SUBSTR(DEST.codon, 1,1), \"C\", SUBSTR(DEST.codon, 3,3)) " + 
	"              WHEN DEST.codon_pos=\"3\" THEN CONCAT(SUBSTR(DEST.codon, 1,2), \"C\") " + 
	"          END " + 
	"              " + 
	"          END " + 
	"              WHEN DEST.strand=\"+\" THEN CASE " + 
	"              WHEN DEST.codon_pos=\"1\" THEN CONCAT(SRC.alternate_bases, SUBSTR(DEST.codon, 2,3)) " + 
	"              WHEN DEST.codon_pos=\"2\" THEN CONCAT(SUBSTR(DEST.codon, 1,1), SRC.alternate_bases, SUBSTR(DEST.codon, 3,3)) " + 
	"              WHEN DEST.codon_pos=\"3\" THEN CONCAT(SUBSTR(DEST.codon, 1,2), SRC.alternate_bases) " + 
	"          END " + 
	" " + 
	"             " + 
	"      END " + 
	"       ELSE  " + 
	"                  CASE " + 
	"                      WHEN DEST.codon_pos=\"1\" THEN CONCAT(SRC.alternate_bases, DEST.codon) " + 
	"                      WHEN DEST.codon_pos=\"2\" THEN CONCAT(SUBSTR(DEST.codon, 1,1), SRC.alternate_bases, SUBSTR(DEST.codon, 2,3)) " + 
	"                      WHEN DEST.codon_pos=\"3\" THEN CONCAT(SUBSTR(DEST.codon, 1,2), SRC.alternate_bases, SUBSTR(DEST.codon, 3,3)) " + 
	"                  END " + 
	"       " + 
	"    END " + 
	"      AS NewCodon " + 
	"    FROM ( " + 
	"      SELECT " + 
	"        reference_name, " + 
	"        case  " + 
	"         WHEN SUBSTR(reference_bases, 0, LENGTH(alternate_bases)) = alternate_bases THEN start_position+LENGTH(alternate_bases) " + 
	"         ELSE start_position " + 
	"         END start_position, " + 
	"        end_position, " + 
	"        CASE " + 
	"          WHEN reference_bases = '.' THEN '' " + 
	"          WHEN SUBSTR(alternate_bases, 0, LENGTH(reference_bases)) = reference_bases THEN '' " + 
	"          WHEN SUBSTR(reference_bases, 0, LENGTH(alternate_bases)) = alternate_bases THEN  " + 
	"          SUBSTR(reference_bases, LENGTH(alternate_bases)+1, LENGTH(reference_bases)-LENGTH(alternate_bases)+1) " + 
	"        ELSE " + 
	"        reference_bases " + 
	"      END " + 
	"        AS reference_bases, " + 
	"        CASE " + 
	"          WHEN LENGTH(alternate_bases)<=LENGTH(reference_bases) THEN CASE " + 
	"          WHEN alternate_bases='.' THEN '' " + 
	"          WHEN SUBSTR(reference_bases, 0, LENGTH(alternate_bases)) = alternate_bases THEN '' " + 
	"        ELSE " + 
	"        alternate_bases " + 
	"      END " + 
	"        ELSE " + 
	"        CASE " + 
	"          WHEN alternate_bases='.' THEN '' " + 
	"          WHEN SUBSTR(alternate_bases, 0, LENGTH(reference_bases)) = reference_bases THEN SUBSTR(alternate_bases, LENGTH(reference_bases)+1, LENGTH(alternate_bases)-LENGTH(reference_bases)+1) " + 
	"        ELSE " + 
	"        alternate_bases " + 
	"      END " + 
	"      END " + 
	"        AS alternate_bases " + 
	"        FROM " + 
	"        `"+ VCFTempTable +"`) AS SRC " + 
	"    JOIN " + 
	"       `gbsc-gcp-project-cba.AnnotationHive_hg19.hg19_refGene_Mrna_0base` AS DEST " + 
	"    ON " + 
	"      SRC.reference_name=DEST.reference_name AND SRC.start_position=DEST.start_position " + 
	" ) AS GetCodon " + 
	"  JOIN " + 
	"    `gbsc-gcp-project-cba.annotation.AminoAcid` AS DEST " + 
	"  ON " + 
	"    GetCodon.codon=DEST.start_codon " + 
	"  GROUP BY " + 
	"    reference_name, " + 
	"    start_position, " + 
	"    end_position, " + 
	"    reference_bases, " + 
	"    alternate_bases, " + 
	"    syn, " + 
	"    codon, " + 
	"    NewCodon " + 
	"    ) " + 
	"         " + 
	"    GROUP BY " + 
	"  reference_name, " + 
	"  start_position, " + 
	"  end_position, " + 
	"  reference_bases, " + 
	"  alternate_bases, " + 
	"  codon, " + 
	"  NewCodon " + 
	"  )) AS DEST" + 
	" ON" + 
	"  DEST.reference_name=SRC.reference_name" + 
	"  AND DEST.start_position=SRC.start_position" + 
	"  AND DEST.end_position=SRC.end_position" + 
	"  AND DEST.reference_bases= SRC.reference_bases" + 
	"  AND DEST.alternate_bases= SRC.alternate_bases"  ;	
	return tempQuery;
}
    
}


