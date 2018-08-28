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


		String queryStat = "SELECT *, LENGTH(innerQ.Chromosome) as len from (SELECT  chrm as Chromosome, "
				+ "count(*) as Cnt, min (start) as Minimum, max(start) as Maximum FROM   " + "[" + projectId + ":"
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
				String chrm = row.get(0).getValue().toString();
				if (!chrm.equals("M") && !chrm.equals("MT") && !chrm.equals("X") && !chrm.equals("Y")) {
					System.out.println(chrm + " " + row.get(1).getValue().toString() + " "
							+ row.get(2).getValue().toString() + " " + row.get(3).getValue().toString());
					partition(row.get(0).getValue().toString(),
							Integer.parseInt(row.get(1).getValue().toString()),
							Integer.parseInt(row.get(2).getValue().toString()),
							Integer.parseInt(row.get(3).getValue().toString()), projectId, datasetId, tableId,
							outputFile, BinSize);
				} else {
					if (chrm.equals("M") || chrm.equals("MT"))
						M = row;
					else if (chrm.equals("X"))
						X = row;
					else if (chrm.equals("Y"))
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
				+ "where chrm='" + chromId + "' and "
				 + startMin + "<= start and start <= " 
				 + startMax 
				+ " order by chrm, start";
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

	public static void runQueryPermanentTable(String queryString, String destinationDataset, String destinationTable,
			boolean allowLargeResults, int MaximumBillingTier, boolean LegacySql, boolean Update) throws TimeoutException, InterruptedException {
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
				queryConfig = QueryJobConfiguration.newBuilder(queryString)
				.setDestinationTable(TableId.of(destinationDataset, destinationTable))
				.setAllowLargeResults(allowLargeResults)
				.setUseLegacySql(LegacySql)
				.build();
			}
		}
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
	public static void deleteTable(String destinationDataset, String destinationTable){
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
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
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, `END`, reference_bases, alternate_bases ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, `END`, reference_bases, alt as alternate_bases ";

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
												
						AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, " + RequestedFields
							+ " FROM " + VCFQuery 
							+ " JOIN `" + TableName +"` AS " + AliasTableName ;
						AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start = VCF.start) AND (" + AliasTableName + ".END = VCF.END) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alt) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alt = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".base)))"; // , ";
			}
		}
		
		//TODO:Transcript + SQL Standard		
	
		String Query= "#standardSQL \n SELECT "
				+ " VCF.reference_name as chrm, VCF.start as start, VCF.END as `end`, VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, " + AllFields 
				+ " FROM ";
		
		Query += AnnotationQuery;
		//Query += " group by chrm,  start,  end, reference_bases, alternate_bases, " + GroupByFields; 

		
		return Query;
	}
	
	public static String prepareAnnotateVariantQueryConcatFields_mVCF(String VCFTableNames, String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			  String TranscriptCanonicalizeRefNames, String VariantAnnotationTableNames, 
			  String VariantannotationCanonicalizerefNames, 
			  boolean createVCF, boolean TempVCF, boolean GoogleVCF, boolean numSamples) {

				
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
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, END, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END, reference_bases, alternate_bases ";
			
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
							"  start," + 
							"  END," + 
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
							"  start," + 
							"  END," + 
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
							
							AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases ";
							if (numSamples && GoogleVCF) {
								AnnotationQuery += ", num_samples ";
							}
							
							if(createVCF)
								AnnotationQuery += ", CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
							else 
								AnnotationQuery += ", " + RequestedFields ;

						}
						else {
							AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, ";
							if (numSamples && GoogleVCF) {
								AnnotationQuery += " num_samples, ";
							}
							AnnotationQuery +=  RequestedFields;
						}
						
						AnnotationQuery +=
							 " FROM " + VCFQuery 
							+ " JOIN [" + TableName +"] AS " + AliasTableName ;
						AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start = VCF.start) AND (" + AliasTableName + ".END = VCF.END) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alt) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alt = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".base)) ";
						
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
						if (TableInfo.length>3){ //Top Select 
							if(VariantAnnotationTables!=null) {
									AllFields += ", CONCAT(\""+ (index+1+VariantAnnotationTables.length) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
							}
							else {
									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
							}
						}
						else{
							if(VariantAnnotationTables!=null) {
									AllFields += ", CONCAT(\""+ (index+1+VariantAnnotationTables.length) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
								
							}
							else	 {
									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";								
							}
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
					AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, "
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
					AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, ";
					if (numSamples && GoogleVCF) {
						AnnotationQuery += " num_samples, ";
					}	
					AnnotationQuery += RequestedFields;
				}
				AnnotationQuery +=
					 " FROM " + VCFQuery 
					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";


				AnnotationQuery += " WHERE "
						+ " ("+ AliasTableName +".start <= VCF.END) AND (VCF.start <= "+ AliasTableName +".end ) ";				
				
				if(index+1 <  TranscriptAnnotations.length)
					AnnotationQuery +=  "), ";
				else
					AnnotationQuery +=  ") ";
			
			}
		}		
	
		if (numSamples && GoogleVCF) {
			AllFields = ",  num_samples " + AllFields ;
		}
		String Query= "  SELECT "
				+ " VCF.reference_name as chrm, VCF.start as start, VCF.END as end, VCF.reference_bases as reference_bases, "
				+ "VCF.alternate_bases as alternate_bases " + AllFields 
				+ " FROM ";
		
		Query += AnnotationQuery;
		
		if(!createVCF && GroupBy) {
			Query += " GROUP BY  chrm, start, END, reference_bases, alternate_bases "; 
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
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, END ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END ";

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
												
						AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases " + RequestedFields
							+ " FROM " + VCFQuery 
							+ " JOIN [" + TableName +"] AS " + AliasTableName ;
						AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start = VCF.start) AND (" + AliasTableName + ".END = VCF.END) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alt) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alt = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".base)) ";
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
										
				AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END ";
						if(VariantAnnotationTableNames!=null){
							AnnotationQuery += ", VCF.reference_bases, VCF.alternate_bases " ;
						}
				AnnotationQuery +=RequestedFields
					+ " FROM " + VCFQuery 
					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";

				AnnotationQuery += " WHERE "			
				//		x1 <= y2 && y1 <= x2
				+ "((("+ AliasTableName +".start <= VCF.end) AND ("+ AliasTableName +".end >= VCF.start)))";

		
				if(index+1 <  TranscriptAnnotations.length)
					AnnotationQuery +=  "), ";
				else
					AnnotationQuery += ") ";
			}
		
		}		
	
		String Query= "  SELECT VCF.reference_name as chrm, VCF.start as start, VCF.END as end ";
				if(VariantAnnotationTableNames!=null){
					 Query+= ", VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases ";
				}
				
		Query += AllFields + " FROM ";
		
		Query += AnnotationQuery;
		if (!createVCF) {
			Query += " GROUP BY  chrm, start, END "; 
			
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
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, END, reference_bases, alternate_bases,  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END, reference_bases, alternate_bases, ";

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

								
				QueryCalcAll += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases " + RequestedFields + ", AN.start, AN.end , "
						+ "CASE WHEN (ABS(VCF.End-AN.Start) >= ABS(VCF.Start - AN.End)) THEN ABS(VCF.Start-AN.End) ELSE ABS(VCF.End-AN.Start) END as distance "
						+ " FROM " + VCFQuery 
					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				QueryCalcAll += " ON " + AliasTableName + ".chrm = VCF.reference_name"
						+ " ) as CalcALL ";

				
//				QueryCalcMin += " ( SELECT VCF.reference_name, VCF.start, VCF.END, MIN(CASE "
//						+ "WHEN (ABS(VCF.End-AN.Start) >= ABS(VCF.Start - AN.End)) THEN ABS(VCF.Start-AN.End) " + 
//						" ELSE ABS(VCF.End-AN.Start) END) as distance "
//						+ " FROM " + VCFQuery 
//						+ " JOIN [" + TableName +"] AS " + AliasTableName ;
//				QueryCalcMin += " ON " + AliasTableName + ".chrm = VCF.reference_name "
//						+ "Group By VCF.reference_name, VCF.start, VCF.END "
//						+ " ) as CalcMin ";
//				
				AnnotationQuery += QueryCalcAll;
				AnnotationQuery += "join ";
//				AnnotationQuery += QueryCalcMin;		
				AnnotationQuery += "(SELECT     VCF_reference_name,     VCF_start,     VCF_END,      distance   FROM     [" + TempMinTableName +"] ) AS CalcMin "; 

				AnnotationQuery += " ON " + 
						"  CalcALL.VCF.reference_name = CalcMin.VCF_reference_name " + 
						"  AND CalcALL.VCF.start = CalcMin.VCF_start " + 
						"  AND CalcALL.VCF.END = CalcMin.VCF_END " + 
						"  AND CalcALL.distance = CalcMin.distance " ;

				
//				AnnotationQuery += "ON" + 
//						"  CalcALL.VCF.reference_name = CalcMin.VCF.reference_name " + 
//						"  AND CalcALL.VCF.start = CalcMin.VCF.start " + 
//						"  AND CalcALL.VCF.END = CalcMin.VCF.END " + 
//						"  AND CalcALL.distance = CalcMin.distance " ;
						
				
		String Query= "  SELECT "
				+ " CalcALL.VCF.reference_name as chrm, CalcALL.VCF.start as start, CalcALL.VCF.END as end, "
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
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name ";

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

								
				QueryCalcAll += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases " + RequestedFields + ", AN.start, AN.end , ABS(VCF.start-AN.Start) as distance "
					+ " FROM " + VCFQuery 
					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				QueryCalcAll += " ON " + AliasTableName + ".chrm = VCF.reference_name) as CalcALL ";

				
				QueryCalcMin += " ( SELECT VCF.reference_name, VCF.start, VCF.END, MIN(ABS(VCF.start-AN.Start)) as distance "
						+ " FROM " + VCFQuery 
						+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				QueryCalcMin += " ON " + AliasTableName + ".chrm = VCF.reference_name "
						+ "Group By VCF.reference_name, VCF.start, VCF.END "
						+ " ) as CalcMin ";
				
				AnnotationQuery += QueryCalcAll;
				AnnotationQuery += "join ";
				AnnotationQuery += QueryCalcMin;		
				
				AnnotationQuery += "ON" + 
						"  CalcALL.VCF.reference_name = CalcMin.VCF.reference_name " + 
						"  AND CalcALL.VCF.start = CalcMin.VCF.start " + 
						"  AND CalcALL.VCF.END = CalcMin.VCF.END " + 
						"  AND CalcALL.distance = CalcMin.distance " ;
						
				
		String Query= "  SELECT "
				+ " CalcALL.VCF.reference_name as chrm, CalcALL.VCF.start as start, CalcALL.VCF.END as end, "
				+ "CalcALL.VCF.reference_bases as reference_bases, CalcALL.VCF.alternate_bases as alternate_bases " + AllFields +  ", CalcALL.distance, "
				+ "  CASE " + 
				"    WHEN " + 
				"        ((CalcALL.AN.start <= CalcALL.VCF.END) AND (CalcALL.VCF.start <= CalcALL.AN.END)) " + 
				"       THEN " + 
				"          'Overlapped' " + 
				"       ELSE " + 
				"          'Closest' " + 
				"  END AS Status " + 
				" FROM ";
		
		Query += AnnotationQuery;
	
		if (OnlyIntrogenic) {
			Query += " WHERE (( CalcALL.VCF.start> CalcALL.AN.End) OR (CalcALL.AN.Start> CalcALL.VCF.END))";
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
						+ "', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name ";

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

		String Query = "  SELECT " + " VCF.reference_name as chrm, VCF.start as start, VCF.END as end, "
				+ "VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases " + TopFields
				+ ", AN.start" + ", AN.end" + ", CASE"
				+ "      WHEN (ABS(VCF.End-AN.Start)>=ABS(VCF.Start - AN.End)) THEN ABS(VCF.Start-AN.End)"
				+ "      ELSE ABS(VCF.End-AN.Start)" + "    END AS distance" + ", CASE "
				+ "    WHEN ((VCF.start <= AN.End) AND (AN.Start <= VCF.END)) THEN 'Overlapped' "
				+ "    ELSE 'Closest' " + "  END AS Status " + " FROM " + VCFQuery + " JOIN (" + "SELECT "
				+ "      chrm " + "      , start  " + "      , END " + RequestedFields + " From " + "[" + TableName
				+ "]) AS " + AliasTableName + " ON " + AliasTableName + ".chrm = VCF.reference_name ";

		Query = "Select * from (" + Query + ") WHERE distance < " + Threashold;

		if (onlyIntrogenic) {
			Query += " AND (( VCF.start> AN.End) OR (AN.Start> VCF.END))";
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
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name, quality  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name, quality ";

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
				+ " VCF.reference_name as chrm, VCF.start as start, VCF.END as end, "
				+ "VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases " 
				+ TopFields
				+ ", AN.start"
				+ ", AN.end"
				+ ", CASE" 
				+ "      WHEN (ABS(VCF.End-AN.Start)>=ABS(VCF.Start - AN.End)) THEN ABS(VCF.Start-AN.End)"  
				+ "      ELSE ABS(VCF.End-AN.Start)" 
				+ "    END AS distance"
				+ ", CASE "
				+ "    WHEN ((VCF.start <= AN.End) AND (AN.Start <= VCF.END)) THEN 'Overlapped' "
				+ "    ELSE 'Closest' "  
				+ "  END AS Status "
				+ " FROM " + VCFQuery
				+ " JOIN ("
				+ "SELECT " 
				+ "      chrm "  
				+ "      , start  "  
				+ "      , END "  
				+ RequestedFields
				+ " From "
				+ "[" + TableName +"]) AS " + AliasTableName
				+ " ON " + AliasTableName + ".chrm = VCF.reference_name ";
		if (onlyIntrogenic) {
			Query += " WHERE (( VCF.start> AN.End) OR (AN.Start> VCF.END))";
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
							+ " start > " + RS[1] + " AND start < " + RS[2] + ") OR " ;
				}
				else {
					WHERE_VCF += " (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start > " + RS[1] + " AND start < " + RS[2] + ") " ;
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
						+ "', '') as reference_name, start, `END`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, `END`, reference_bases, alternate_bases ";

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
//			Query = "SELECT " + "    VCF.reference_name as chrm," + "    VCF.start as start," + "    VCF.END as `end`,"
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
					"  chrm," + 
					"  start," + 
					"  `end`," + 
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
					"    VCF.reference_name AS chrm," + 
					"    VCF.start AS start," + 
					"    VCF.END AS `end`," + 
					"    VCF.reference_bases AS reference_bases," + 
					"    ARRAY_TO_STRING(VCF.alternate_bases, ',') AS alternate_bases," + 
					"    ARRAY_AGG ((CONCAT(AN.name, \";\" , AN.name2, \";\", \"dist:\", CAST((CASE" + 
					"          WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.END)) AND "
					+ "(ABS(VCF.start-AN.Start) >= ABS(VCF.Start - AN.END)) AND "
					+ "(ABS(VCF.END-AN.END) >= ABS(VCF.Start - AN.END))) THEN ABS(VCF.Start-AN.END)" + 
					"          WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.start))" + 
					"          AND (ABS(VCF.END-AN.END) >= ABS(VCF.Start - AN.start))) THEN ABS(VCF.Start-AN.Start)" + 
					"          WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.END - AN.END))) THEN ABS(VCF.END-AN.END)" + 
					"          ELSE ABS(VCF.END-AN.Start) END) as STRING) ))" + 
					"    ORDER BY" + 
					"      (CASE" + 
					"          WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.END)) "
					+ "AND (ABS(VCF.start-AN.Start) >= ABS(VCF.Start - AN.END)) AND "
					+ "(ABS(VCF.END-AN.END) >= ABS(VCF.Start - AN.END))) THEN ABS(VCF.Start-AN.END)" + 
					"          WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.start))" + 
					"          AND (ABS(VCF.END-AN.END) >= ABS(VCF.Start - AN.start))) THEN ABS(VCF.Start-AN.Start)" + 
					"          WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.END - AN.END))) THEN ABS(VCF.END-AN.END)" + 
					"          ELSE ABS(VCF.END-AN.Start) END)" + 
					"    LIMIT\n" + 
					"      1 )[SAFE_OFFSET(0)] names," + 
					"    CASE" + 
					"      WHEN ((AN.start > VCF.END) OR ( VCF.start > AN.END)) THEN '3Intergenic'" + 
					"      WHEN ((AN.StartPoint <= VCF.END)" + 
					"      AND (VCF.start <= AN.EndPoint)) THEN '1Exonic'" + 
					"      ELSE '2Intronic'" + 
					"    END AS Status "	
					+ " FROM " + VCFQuery + " JOIN (\n" + 
							"    SELECT " + 
							"      chrm," + 
							"      start, " + 
							"      `end`, " + 
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
							"   ON VCF.reference_name = AN.chrm ";
			if (onlyIntrogenic)
				Query += " WHERE (VCF.start>AN.END) OR (AN.Start> VCF.END) ";
			Query += "   GROUP BY" + 
					"    VCF.reference_name," + 
					"    VCF.start," + 
					"    VCF.END," + 
					"    VCF.reference_bases," + 
					"    alternate_bases," + 
					"    Status )" + 
					"GROUP BY" + 
					"  chrm," + 
					"  start," + 
					"  `end`," + 
					"  reference_bases," + 
					"  alternate_bases";

			// :TODO M and MT case
		} else {
			Query = "  SELECT " + " VCF.reference_name as chrm, VCF.start as start, VCF.END as `end`, "
					+ "VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, " + AllFields
					+ ", AN_start" + ", AN_end" + ", CASE"
					+ "      WHEN (ABS(VCF.End-AN_Start)>=ABS(VCF.Start - AN_End)) THEN ABS(VCF.Start-AN_End)"
					+ "      ELSE ABS(VCF.End-AN_Start)" + "    END AS distance" + ", CASE "
					+ "    WHEN ((VCF.start <= AN_End) AND (AN_Start <= VCF.END)) THEN 'Overlapped' "
					+ "    ELSE 'Closest' " + "  END AS Status " + " FROM " + VCFQuery + " JOIN (" + "SELECT "
					+ "      chrm " + "      , start as AN_Start  " + "      , `END` as AN_END " + RequestedFields
					+ " From " + "`" + TableName + "`) AS AN " + " ON AN.chrm = VCF.reference_name ";
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
							+ " start > " + RS[1] + " AND start < " + RS[2] + ") OR " ;
				}
				else {
					WHERE_VCF += " AND (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start > " + RS[1] + " AND start < " + RS[2] + ") " ;
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
						+ "', '') as reference_name, start, `END`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, `END`, reference_bases, alternate_bases ";

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
			Query = "SELECT " + "    VCF.reference_name as chrm," + "    VCF.start as start," + "    VCF.END as `end`,"
					+ "    VCF.reference_bases as reference_bases,"
					+ "    ARRAY_TO_STRING(VCF.alternate_bases, ',') as alternate_bases," + "	   ARRAY_AGG(("
					+ AllFields + ")" + "    ORDER BY "
					+ "     (CASE         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.END)) AND         "
					+ " (ABS(VCF.start-AN.Start) >= ABS(VCF.Start - AN.END)) AND         "
					+ " (ABS(VCF.end-AN.end) >= ABS(VCF.Start - AN.END)))         "
					+ "  THEN ABS(VCF.Start-AN.END)         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.start)) AND         "
					+ "(ABS(VCF.end-AN.end) >= ABS(VCF.Start - AN.start)))          "
					+ "THEN ABS(VCF.Start-AN.Start)         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.end - AN.end)))          "
					+ "THEN ABS(VCF.END-AN.END)         ELSE ABS(VCF.END-AN.Start) END) " + "    LIMIT "
					+ "      1 )[SAFE_OFFSET(0)] names " + ", CASE " + "		 WHEN "
					+ "        ((AN.start >  VCF.END) OR ( VCF.start > AN.END)) " + "        THEN "
					+ "        'Closest' " + "        ELSE " + "        'Overlapped'" + "       END AS Status "
					+ " FROM " + VCFQuery + " JOIN `" + TableName + "` AS AN " + "  ON VCF.reference_name = AN.chrm ";
			if (onlyIntrogenic)
				Query += " WHERE (VCF.start>AN.END) OR (AN.Start> VCF.END) ";
			Query += " GROUP BY VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, alternate_bases, Status";

			// :TODO M and MT case
		} else {
			Query = "  SELECT " + " VCF.reference_name as chrm, VCF.start as start, VCF.END as `end`, "
					+ "VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, " + AllFields
					+ ", AN_start" + ", AN_end" + ", CASE"
					+ "      WHEN (ABS(VCF.End-AN_Start)>=ABS(VCF.Start - AN_End)) THEN ABS(VCF.Start-AN_End)"
					+ "      ELSE ABS(VCF.End-AN_Start)" + "    END AS distance" + ", CASE "
					+ "    WHEN ((VCF.start <= AN_End) AND (AN_Start <= VCF.END)) THEN 'Overlapped' "
					+ "    ELSE 'Closest' " + "  END AS Status " + " FROM " + VCFQuery + " JOIN (" + "SELECT "
					+ "      chrm " + "      , start as AN_Start  " + "      , `END` as AN_END " + RequestedFields
					+ " From " + "`" + TableName + "`) AS AN " + " ON AN.chrm = VCF.reference_name ";
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
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name, quality  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name, quality ";

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
					
				
				String Query = "SELECT   VCF.reference_name,   VCF.start,   VCF.END,   MIN(CASE     WHEN (ABS(VCF.END-AN.Start)>=ABS(VCF.Start - AN.END)) "
							+ " THEN ABS(VCF.Start - AN.END)     ELSE ABS(VCF.END-AN.Start)   END) AS distance FROM "+ VCFQuery 
							+ " JOIN [" + TableName +"] AS AN ON  AN.chrm = VCF.reference_name "
							+ " WHERE (( VCF.start> AN.END)  OR (AN.Start> VCF.END)) "
							+ " Group By VCF.reference_name,   VCF.start,   VCF.END ";		

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
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, `END`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, `END`, reference_bases, alternate_bases ";

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
						+ "    VCF.reference_name as chrm," 
						+ "    VCF.start as start,"  
						+ "    VCF.END as `end`," 
						+ "    VCF.reference_bases as reference_bases," 
						+ "    ARRAY_TO_STRING(VCF.alternate_bases, ',') as alternate_bases,"
						+ "	   ARRAY_AGG((" + AllFields +")"
						+ "    ORDER BY " 
//						"      (CASE " + 
//						"          WHEN (ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.END)) THEN ABS(VCF.Start-AN.END) " + 
//						"          ELSE ABS(VCF.END-AN.Start) END) "
						+ "     (CASE         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.END)) AND         "
						+ " (ABS(VCF.start-AN.Start) >= ABS(VCF.Start - AN.END)) AND         "
						+ " (ABS(VCF.end-AN.end) >= ABS(VCF.Start - AN.END)))         "
						+ "  THEN ABS(VCF.Start-AN.END)         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.start)) AND         "
						+ "(ABS(VCF.end-AN.end) >= ABS(VCF.Start - AN.start)))          "
						+ "THEN ABS(VCF.Start-AN.Start)         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.end - AN.end)))          "
						+ "THEN ABS(VCF.END-AN.END)         ELSE ABS(VCF.END-AN.Start) END) "
						+ "    LIMIT " 
						+ "      1 )[SAFE_OFFSET(0)] names "
						+", CASE " + 
						"		 WHEN " + 
						"        ((AN.start >  VCF.END) OR ( VCF.start > AN.END)) " + 
						"        THEN " + 
						"        'Closest' " + 
						"        ELSE " + 
						"        'Overlapped'" + 
						"       END AS Status "
						+ " FROM " + VCFQuery 
						+ " JOIN `"+ TableName +"` AS AN "
						+ "  ON VCF.reference_name = AN.chrm ";
						if (OnlyIntrogenic)
							Query += " WHERE (VCF.start>AN.END) OR (AN.Start> VCF.END) ";
						Query += " GROUP BY VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, alternate_bases, Status";
						
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
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, `END`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, `END`, reference_bases, alternate_bases ";

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
				+ "    VCF.reference_name as chrm," 
				+ "    VCF.start as start,"  
				+ "    VCF.END as `end`," 
				+ "    VCF.reference_bases as reference_bases," 
				+ "    ARRAY_TO_STRING(VCF.alternate_bases, ',') as alternate_bases,"
				+ "	   ARRAY_AGG((" + AllFields +")"
				+ "    ORDER BY " 
//				"      (CASE " + 
//				"          WHEN (ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.END)) THEN ABS(VCF.Start-AN.END) " + 
//				"          ELSE ABS(VCF.END-AN.Start) END) "
				+ "     (CASE         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.END)) AND         "
				+ " (ABS(VCF.start-AN.Start) >= ABS(VCF.Start - AN.END)) AND         "
				+ " (ABS(VCF.end-AN.end) >= ABS(VCF.Start - AN.END)))         "
				+ "  THEN ABS(VCF.Start-AN.END)         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.start)) AND         "
				+ "(ABS(VCF.end-AN.end) >= ABS(VCF.Start - AN.start)))          "
				+ "THEN ABS(VCF.Start-AN.Start)         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.end - AN.end)))          "
				+ "THEN ABS(VCF.END-AN.END)         ELSE ABS(VCF.END-AN.Start) END) "
				+ "    LIMIT " 
				+ "      1 )[SAFE_OFFSET(0)] names "
				+", CASE " + 
				"		 WHEN " + 
				"        ((AN.start >  VCF.END) OR ( VCF.start > AN.END)) " + 
				"        THEN " + 
				"        'Closest' " + 
				"        ELSE " + 
				"        'Overlapped'" + 
				"       END AS Status "
				+ " FROM " + VCFQuery 
				+ " JOIN `"+ TableName +"` AS AN "
				+ "  ON VCF.reference_name = AN.chrm ";
				if (OnlyIntrogenic)
					Query += " WHERE (VCF.start>AN.END) OR (AN.Start> VCF.END) ";
				Query += " GROUP BY VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, "
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
							+ " start > " + RS[1] + " AND start < " + RS[2] + ") OR " ;
				}
				else {
					WHERE_VCF += " AND (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start > " + RS[1] + " AND start < " + RS[2] + ") " ;
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
						+ "', '') as reference_name, start, `END`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, `END`, reference_bases, alternate_bases ";

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
			Query = "SELECT " + "    VCF.reference_name as chrm," + "    VCF.start as start," + "    VCF.END as `end`,"
					+ "    VCF.reference_bases as reference_bases,"
					+ "    ARRAY_TO_STRING(VCF.alternate_bases, ',') as alternate_bases," + "	   ARRAY_AGG(("
					+ AllFields + ")" + "    ORDER BY "
					+ "     (CASE         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.END)) AND         "
					+ " (ABS(VCF.start-AN.Start) >= ABS(VCF.Start - AN.END)) AND         "
					+ " (ABS(VCF.end-AN.end) >= ABS(VCF.Start - AN.END)))         "
					+ "  THEN ABS(VCF.Start-AN.END)         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.Start - AN.start)) AND         "
					+ "(ABS(VCF.end-AN.end) >= ABS(VCF.Start - AN.start)))          "
					+ "THEN ABS(VCF.Start-AN.Start)         WHEN ((ABS(VCF.END-AN.Start) >= ABS(VCF.end - AN.end)))          "
					+ "THEN ABS(VCF.END-AN.END)         ELSE ABS(VCF.END-AN.Start) END) " + "    LIMIT "
					+ "      1 )[SAFE_OFFSET(0)] names " + ", CASE " + "		 WHEN "
					+ "        ((AN.start >  VCF.END) OR ( VCF.start > AN.END)) " + "        THEN "
					+ "        'Closest' " + "        ELSE " + "        'Overlapped'" + "       END AS Status "
					+ " FROM " + VCFQuery + " JOIN `" + TableName + "` AS AN " + "  ON VCF.reference_name = AN.chrm ";
			if (onlyIntrogenic)
				Query += " WHERE (VCF.start>AN.END) OR (AN.Start> VCF.END) ";
			Query += " GROUP BY VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, alternate_bases, Status";

			// :TODO M and MT case
		} else {

			Query = "  SELECT " + " VCF.reference_name as chrm, VCF.start as start, VCF.END as `end`, "
					+ "VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, " + AllFields
					+ ", AN_start" + ", AN_end" + ", CASE"
					+ "      WHEN (ABS(VCF.End-AN_Start)>=ABS(VCF.Start - AN_End)) THEN ABS(VCF.Start-AN_End)"
					+ "      ELSE ABS(VCF.End-AN_Start)" + "    END AS distance" + ", CASE "
					+ "    WHEN ((VCF.start <= AN_End) AND (AN_Start <= VCF.END)) THEN 'Overlapped' "
					+ "    ELSE 'Closest' " + "  END AS Status " + " FROM " + VCFQuery + " JOIN (" + "SELECT "
					+ "      chrm " + "      , start as AN_Start  " + "      , `END` as AN_END " + RequestedFields
					+ " From " + "`" + TableName + "`) AS AN " + " ON AN.chrm = VCF.reference_name ";
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
		
		String chrm= "\"" + va[0] + "\"";
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
							AnnotationQuery += " ( SELECT " + chrm + ", " + start + "," + end + ", " + RefBases + ","+ AltBase + ", "
									+ "CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
						else
							AnnotationQuery +=  " ( SELECT " + chrm + ", " + start + "," + end + ", " + RefBases + ","+ AltBase + ", "
								    + RequestedFields;

						AnnotationQuery +=
							 " FROM [" + TableName +"] AS " + AliasTableName ;
						AnnotationQuery += " WHERE (" + AliasTableName + ".chrm = " + chrm + ") "
							+ " AND (" + AliasTableName + ".start = " + start + ") AND (" + AliasTableName + ".END = " + end + ") "
							+ " AND (((CONCAT(" + RefBases.replace("-", "") + ", " + AliasTableName + ".alt) =" + AltBase.replace("-", "") +" ) "
							+ " OR " + AliasTableName + ".alt = " + AltBase.replace("-", "") + ") AND (" + RefBases.replace("-", "") +" = " + AliasTableName + ".base)) ";
						
						//This is the case when we have transcript annotations 
						if(index+1 <  VariantAnnotationTables.length)
							AnnotationQuery +=  "), ";
						else
							AnnotationQuery +=  ") ";
			}

		}

		String Query= "  SELECT "
				+ chrm + " as chrm, " + start + " as start, "+ end + " as end, " + RefBases + " as reference_bases, "
				+   AltBase + " as alternate_bases " + AllFields 
				+ " FROM " + AnnotationQuery;
					
		Query += " GROUP BY  chrm, start, END, reference_bases, alternate_bases " ; //+ AllFields; 
	
		
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
		
		String chrm= "\"" + region[0] + "\"";
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
							AnnotationQuery += " ( SELECT " + chrm + ", " + start + "," + end + ", "
									+ "CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
						else
							AnnotationQuery +=  " ( SELECT " + chrm + ", " + start + "," + end + ", "
								    + RequestedFields;

						AnnotationQuery +=
							 " FROM [" + TableName +"] AS " + AliasTableName ;
						AnnotationQuery += " WHERE (" + AliasTableName + ".chrm = " + chrm + ") "
								+ " AND ("+ AliasTableName +".start <= " + end +") AND (" + start + "<= "+ AliasTableName +".end ) ";				
				
									
						//This is the case when we have transcript annotations 
						if(index+1 <  TranscriptAnnotations.length)
							AnnotationQuery +=  "), ";
						else
							AnnotationQuery +=  ") ";
			}

		}

		String Query= "  SELECT "
				+ chrm + " as chrm, " + start + " as start, "+ end + " as end " + AllFields 
				+ " FROM " + AnnotationQuery;
					
		Query += " GROUP BY  chrm, start, END " ; //+ AllFields; 
	
		
		return Query;
	}
	

	public static String createTempVCFTable(List<String[]> listVA, String TempVCFTable, boolean VariantBased) {
		
		String Query="WITH "+ TempVCFTable +" AS ( ";
		boolean setFields=true;
		for(int index=0; index<listVA.size(); index++)
		{
			String [] va = listVA.get(index);
			String chrm= "\"" + va[0] + "\"";
			int start= (Integer.parseInt(va[1])-1)  ; //Convert to 0-based
			int end=  Integer.parseInt(va[2]);
					
			if (setFields) {
				Query += "SELECT " + chrm + " reference_name, " + start + " start, " + end + " `END` ";
				if (VariantBased) {
					String RefBases= "\"" + va[3] + "\"";
					String AltBase= "\"" + va[4] + "\"";
					Query += ", " + RefBases + " reference_bases ," +  AltBase + " alternate_bases ";
				}
				setFields= false;
			}
			else {
				Query += " \n SELECT " + chrm + ", " + start + ", " + end;
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
			Query += ") \nSELECT reference_name, start, `END`, reference_bases, alternate_bases FROM " + TempVCFTable;
		}
		else {
			Query += ") \nSELECT reference_name, start, `END` FROM " + TempVCFTable;
			
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
							+ " start > " + RS[1] + " AND start < " + RS[2] + ") OR " ;
					
					WHERE_ANN += " (chrm=\"" + RS[0].replace("chr", "")
									+ "\") OR " ;
				}
				else {
					WHERE_VCF += " (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start > " + RS[1] + " AND start < " + RS[2] + ") " ;
					
					WHERE_ANN += " (chrm=\"" + RS[0].replace("chr", "") 
									+ "\")" ;
				}
			}
			
		}
		
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index< VCFTables.length; index++){
			if(VCFCanonicalize!=null)
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name, quality  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END, reference_bases, alternate_bases ";

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
							AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, "
									+ "CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
						else
							AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, "
								    + RequestedFields;

						AnnotationQuery +=" FROM " + VCFQuery;
						
						if (!searchRegions.isEmpty()) {
							AnnotationQuery += " JOIN (SELECT * FROM [" + TableName +"] " 
							+ WHERE_ANN + ") AS " + AliasTableName ;
						}else {
							AnnotationQuery +=" JOIN [" + TableName +"] AS " + AliasTableName ;
						}
						AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start = VCF.start) AND (" + AliasTableName + ".END = VCF.END) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alt) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alt = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".base)) ";
						
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
					AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, "
							+ "VCF.alternate_bases, CONCAT(" + RequestedFields +") as " + AliasTableName +"." + AliasTableName;
				else
					AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, "
						    + RequestedFields;

				AnnotationQuery +=" FROM " + VCFQuery;
					 
				if (!searchRegions.isEmpty()) {
					AnnotationQuery += " JOIN (SELECT * FROM [" + TableName +"] " 
					+ WHERE_ANN + ") AS " + AliasTableName ;
				}else {
						AnnotationQuery += " JOIN [" + TableName +"] AS " + AliasTableName ;
				}
						
				AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";


				AnnotationQuery += " WHERE "
						+ " ("+ AliasTableName +".start <= VCF.END) AND (VCF.start <= "+ AliasTableName +".end ) ";				
				
				if(index+1 <  TranscriptAnnotations.length)
					AnnotationQuery +=  "), ";
				else
					AnnotationQuery +=  ") ";
			
			}
		}		
	
		String Query= "  SELECT "
				+ " VCF.reference_name as chrm, VCF.start as start, VCF.END as end, VCF.reference_bases as reference_bases, "
				+ "VCF.alternate_bases as alternate_bases " + AllFields 
				+ " FROM ";
		
		Query += AnnotationQuery;
		
		if(GroupBy) {
			Query = " SELECT    chrm, start, end, reference_bases, alternate_bases " + Concat_Group 
					+ " FROM ( " + Query
					+ " ) GROUP BY  chrm, start, end, reference_bases, alternate_bases " ; 
		}
		return Query;
	}

	public static void incrementStart(String projectId, String bigQueryDatasetId, String outputBigQueryTable) throws Exception  {
	
		String queryStat = "UPDATE   " + "`" + projectId + "."
				+ bigQueryDatasetId + "." + outputBigQueryTable + "` "
				+ " SET start = start + 1 WHERE chrm <>\"\" ";
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
							+ " start > " + RS[1] + " AND start < " + RS[2] + ") OR " ;
					
					WHERE_ANN += " (chrm=\"" + RS[0].replace("chr", "")
									+ "\") OR " ;
				}
				else {
					WHERE_VCF += " (reference_name=\"" + RS[0].replace("chr", "") + "\" AND "
							+ " start > " + RS[1] + " AND start < " + RS[2] + ") " ;
					
					WHERE_ANN += " (chrm=\"" + RS[0].replace("chr", "") 
									+ "\")" ;
				}
			}
			
		}
				
		/*#######################Prepare VCF queries#######################*/
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index < VCFTables.length; index++){
			if(VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, END ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END ";

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
												
						AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases " + RequestedFields
							+ " FROM " + VCFQuery;
						
						if (!searchRegions.isEmpty()) {
							AnnotationQuery += " JOIN (SELECT * FROM [" + TableName +"] " 
							+ WHERE_ANN + ") AS " + AliasTableName ;
						}else {
							AnnotationQuery +=" JOIN [" + TableName +"] AS " + AliasTableName ;
						}
						
						AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start = VCF.start) AND (" + AliasTableName + ".END = VCF.END) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alt) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alt = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".base)) ";
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
										
				AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END ";
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
						
				AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";
				
				
				AnnotationQuery += " WHERE "			
				//		x1 <= y2 && y1 <= x2
				+ "((("+ AliasTableName +".start <= VCF.end) AND ("+ AliasTableName +".end >= VCF.start)))";

		
				if(index+1 <  TranscriptAnnotations.length)
					AnnotationQuery +=  "), ";
				else
					AnnotationQuery += ") ";
			}
		
		}		
	
		String Query= "  SELECT VCF.reference_name as chrm, VCF.start as start, VCF.END as end ";
				//if(VariantAnnotationTableNames!=null){
					 Query+= ", VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases ";
				//}
				
		Query += AllFields + " FROM ";
		
		Query += AnnotationQuery;
		if (GroupBy) {
			Query += " GROUP BY  chrm, start, END "; 
			
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
		 
		String queryString = "SELECT  reference_name as chrm, start, `end`, reference_bases as base, alt, ";
		
		//String queryString = "SELECT  reference_name as chrm, start, `end`, reference_bases as base,   ARRAY_AGG(alt) as alt, ";
		
		String[] samples = numSampleIDs.split(",");
		 
		String SNames = " concat( ";
		String SCount = " ";
		for (int index=0; index<samples.length; index++) {
			if (index+1<samples.length) {
				SNames += " CASE WHEN " + samples[index] + " not like \".%\" Then \"" +  samples[index] + " \" ELSE \"\" END , ";
				SCount += " CASE WHEN " + samples[index] + " not like \".%\" Then 1 ELSE 0 END + ";
			}else {
				SNames += " CASE WHEN " + samples[index] + " not like \".%\" Then \" " +  samples[index] + "\" ELSE \"\" END ) as Sample_Names, ";
				SCount += " CASE WHEN " + samples[index] + " not like \".%\" Then 1 ELSE 0 END as Num_Samples ";
			}
		}
		 
		 queryString += SNames + SCount; // + ", " + otherFields;
		 queryString += " FROM " + tempTableName + ", UNNEST (alternate_bases) as alt ";
		 //queryString += "Group By chrm, start, `end`, base, " + otherFields + ", Sample_Names, Num_Samples";
		 
		LOG.info(queryString);		 
		        
		try {
			BigQueryFunctions.runQueryPermanentTable(queryString, bigQueryDatasetId, outputBigQueryTable,
					allowLargeResults, maximumBillingTier, LegacySql, Update);
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.info("To use " + outputBigQueryTable + " add it to the list of annotation in --variantAnnotationTables (e.g., --variantAnnotationTables=" + project  + ":" + bigQueryDatasetId + "." + bigQueryVCFTableId + ":Sample_Names:Num_Samples)");		 

		
	}
	
	
	
}



