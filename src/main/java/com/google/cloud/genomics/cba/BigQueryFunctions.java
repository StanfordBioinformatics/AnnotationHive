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
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class BigQueryFunctions {
	public static boolean DEBUG = false;

	public static void sort(String projectId, String datasetId, String tableId, String outputFile, int BinSize)
			throws Exception {

//		String queryStat = "SELECT *, LENGTH(innerQ.Chromosome) as len from (SELECT  RIGHT(chrm, LENGTH(chrm) - 3) as Chromosome, "
//				+ "count(*) as Cnt, min (start) as Minimum, max(start) as Maximum FROM   " + "[" + projectId + ":"
//				+ datasetId + "." + tableId + "] "
//				+ "group by Chromosome  order by Chromosome) as innerQ  order by len, innerQ.Chromosome";

		String queryStat = "SELECT *, LENGTH(innerQ.Chromosome) as len from (SELECT  chrm as Chromosome, "
				+ "count(*) as Cnt, min (start) as Minimum, max(start) as Maximum FROM   " + "[" + projectId + ":"
				+ datasetId + "." + tableId + "] "
				+ "group by Chromosome  order by Chromosome) as innerQ  order by len, innerQ.Chromosome";

		
		// Get the results.
		QueryResponse response = runquery(queryStat);
		QueryResult result = response.getResult();

		// Per each chromosome, sort the result.
		List<FieldValue> X = null, Y = null, M = null;
		while (result != null) {
			for (List<FieldValue> row : result.iterateAll()) {
				String chrm = row.get(0).getValue().toString();
				if (!chrm.equals("M") && !chrm.equals("X") && !chrm.equals("Y")) {
					System.out.println(chrm + " " + row.get(1).getValue().toString() + " "
							+ row.get(2).getValue().toString() + " " + row.get(3).getValue().toString());
					sortAndPrint(row.get(0).getValue().toString(),
							Integer.parseInt(row.get(1).getValue().toString()),
							Integer.parseInt(row.get(2).getValue().toString()),
							Integer.parseInt(row.get(3).getValue().toString()), projectId, datasetId, tableId,
							outputFile, BinSize);
				} else {
					if (chrm.equals("M"))
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

			sortAndPrint("chr" + X.get(0).getValue().toString(), Integer.parseInt(X.get(1).getValue().toString()),
					Integer.parseInt(X.get(2).getValue().toString()), Integer.parseInt(X.get(3).getValue().toString()),
					projectId, datasetId, tableId, outputFile, BinSize);

		}
		if (Y != null) {
			System.out.println(Y.get(0).getValue().toString() + " " + Y.get(1).getValue().toString() + " "
					+ Y.get(2).getValue().toString() + " " + Y.get(3).getValue().toString());
			sortAndPrint("chr" + Y.get(0).getValue().toString(), Integer.parseInt(Y.get(1).getValue().toString()),
					Integer.parseInt(Y.get(2).getValue().toString()), Integer.parseInt(Y.get(3).getValue().toString()),
					projectId, datasetId, tableId, outputFile, BinSize);

		}
		if (M != null) {
			System.out.println(M.get(0).getValue().toString() + " " + M.get(1).getValue().toString() + " "
					+ M.get(2).getValue().toString() + " " + M.get(3).getValue().toString());
			sortAndPrint("chr" + M.get(0).getValue().toString(), Integer.parseInt(M.get(1).getValue().toString()),
					Integer.parseInt(M.get(2).getValue().toString()), Integer.parseInt(M.get(3).getValue().toString()),
					projectId, datasetId, tableId, outputFile, BinSize);

		}
	}

	
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
				if (!chrm.equals("M") && !chrm.equals("X") && !chrm.equals("Y")) {
					System.out.println(chrm + " " + row.get(1).getValue().toString() + " "
							+ row.get(2).getValue().toString() + " " + row.get(3).getValue().toString());
					partition(row.get(0).getValue().toString(),
							Integer.parseInt(row.get(1).getValue().toString()),
							Integer.parseInt(row.get(2).getValue().toString()),
							Integer.parseInt(row.get(3).getValue().toString()), projectId, datasetId, tableId,
							outputFile, BinSize);
				} else {
					if (chrm.equals("M"))
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

			partition("chr" + X.get(0).getValue().toString(), Integer.parseInt(X.get(1).getValue().toString()),
					Integer.parseInt(X.get(2).getValue().toString()), Integer.parseInt(X.get(3).getValue().toString()),
					projectId, datasetId, tableId, outputFile, BinSize);

		}
		if (Y != null) {
			System.out.println(Y.get(0).getValue().toString() + " " + Y.get(1).getValue().toString() + " "
					+ Y.get(2).getValue().toString() + " " + Y.get(3).getValue().toString());
			partition("chr" + Y.get(0).getValue().toString(), Integer.parseInt(Y.get(1).getValue().toString()),
					Integer.parseInt(Y.get(2).getValue().toString()), Integer.parseInt(Y.get(3).getValue().toString()),
					projectId, datasetId, tableId, outputFile, BinSize);

		}
		if (M != null) {
			System.out.println(M.get(0).getValue().toString() + " " + M.get(1).getValue().toString() + " "
					+ M.get(2).getValue().toString() + " " + M.get(3).getValue().toString());
			partition("chr" + M.get(0).getValue().toString(), Integer.parseInt(M.get(1).getValue().toString()),
					Integer.parseInt(M.get(2).getValue().toString()), Integer.parseInt(M.get(3).getValue().toString()),
					projectId, datasetId, tableId, outputFile, BinSize);

		}
	}

	
	
	
	
	public static void sortAllAndPrint(String projectId,
			String datasetId, String tableId, String outputFile) {

		String Query = "SELECT  * FROM ";

		
	
		for (int chrmId=1; chrmId<23; chrmId++){
			Query +=  " (SELECT CAST (RIGHT(chrm, LENGTH(chrm) - 3) as INTEGER) as chrm, CAST (start as INTEGER), end, "
					+ "reference_bases, alternate_bases, Annotation_info FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
					+ "where chrm='chr" + chrmId + "'"
					+ " order by chrm, start) as chrm" + chrmId + ", ";

		}
		
		
		Query +=  " (SELECT CAST ('23' as INTEGER) as chrm, CAST (start as INTEGER), end, "
					+ "reference_bases, alternate_bases, Annotation_info FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
				+ "where chrm='chrX'"
				+ " order by chrm, start) as chrmX, ";
		
		Query +=  " (SELECT CAST ('24' as INTEGER) as chrm, CAST (start as INTEGER), end, "
					+ "reference_bases, alternate_bases, Annotation_info FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
				+ "where chrm='chrY'"
				+ " order by chrm, start) as chrmY, ";

		Query +=  " (SELECT CAST ('25' as INTEGER) as chrm, CAST (start as INTEGER), end, "
					+ "reference_bases, alternate_bases, Annotation_info FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
				+ "where chrm='chrM'"
				+ " order by chrm, start) as chrmM ";

		Query += " order by chrm ";
		
		//START - Reset start for BigQuery 
		long startTime = System.currentTimeMillis();
				
		
		//if (DEBUG)
			System.out.println(Query);
		// Get the results.
		QueryResponse response = runquery(Query);
		QueryResult result = response.getResult();
		
		//END - RUN time 
		long tempEstimatedTime = System.currentTimeMillis() - startTime;

		System.out.println("Execution Time for Sort: " + tempEstimatedTime);
		
		//START - Reset start for BigQuery 
		startTime = System.currentTimeMillis();

		
		if (result != null) {
			for (List<FieldValue> row : result.iterateAll()) {
				try {
					 String temp="";
				      for (FieldValue val : row) {
				    	  temp += val.getValue().toString() + "\t";
				        }
				      if(temp.startsWith("23")){
				    	  temp=temp.substring(2);
				    	  temp = "X" + temp;
				      }
				      else if (temp.startsWith("24")){
				    	  temp = temp.substring(2);
				    	  temp = "Y" + temp;
				      }
				      else if (temp.startsWith("25")){
				    	  temp = temp.substring(2);
				    	  temp = "M" + temp;
				      }

				    FileWriter fw = new FileWriter(outputFile, true);    
				    fw.write(temp + "\n"); 
					fw.close();
				} catch (IOException ioe) {
					System.err.println("IOException: " + ioe.getMessage());
				}
				

			}
			
		}
		tempEstimatedTime = System.currentTimeMillis() - startTime;

		System.out.println("Execution Time for Transfer and Print: " + tempEstimatedTime);
	
	}
	
	
	public static void sortAllAndPrintDataflow(String projectId,
			String datasetId, String tableId, String outputFile) {

		String Query = "SELECT chrm, info FROM (SELECT  * FROM ";

		
	
		for (int chrmId=1; chrmId<23; chrmId++){
			Query +=  " (SELECT CAST (RIGHT(chrm, LENGTH(chrm) - 3) as INTEGER) as chrm, start, end, info "
					+ " FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
					+ "where chrm='chr" + chrmId + "'"
					+ " order by chrm, start) as chrm" + chrmId + ", ";
		}

		Query +=  " (SELECT CAST ('23' as INTEGER) as chrm, start, end, info "
					+ "FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
				+ "where chrm='chrX'"
				+ " order by chrm, start) as chrmX, ";
		
		Query +=  " (SELECT CAST ('24' as INTEGER) as chrm, start, end, info "
					+ "FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
				+ "where chrm='chrY'"
				+ " order by chrm, start) as chrmY, ";

		Query +=  " (SELECT CAST ('25' as INTEGER) as chrm, start, end, info "
					+ "FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
				+ "where chrm='chrM'"
				+ " order by chrm, start) as chrmM ";

		//TODO: make sure "order by" scales 
		Query += ") "; // " order by chrm, start) ";
		
		//START - Reset start for BigQuery 
		long startTime = System.currentTimeMillis();
				
		
		//if (DEBUG)
			System.out.println(Query);
		// Get the results.
		QueryResponse response = runquery(Query);
		QueryResult result = response.getResult();
		
		//END - RUN time 
		long tempEstimatedTime = System.currentTimeMillis() - startTime;

		System.out.println("Execution Time for Sort: " + tempEstimatedTime);
		
		//START - Reset start for BigQuery 
		startTime = System.currentTimeMillis();

		//ArrayList<Integer> arrli = new ArrayList<Integer>(n);
		HashMap<Integer, List<String>> OrderBy = new HashMap<Integer, List<String>>();
		for (int index=1; index<=25; index++){
			List<String> L = new ArrayList<String>();
			OrderBy.put(index, L);
		}
		
		
		if (result != null) {
			for (List<FieldValue> row : result.iterateAll()) {
				String str2=(String) row.get(1).getValue();
				Integer chrId= Integer.parseInt((String) row.get(0).getValue());
				//System.out.println("CHID:" +chrId + "  " +str2);
				OrderBy.get(chrId).add(str2);

//				try {
//					 String temp="";
//				      for (FieldValue val : row) {
//				    	  temp += val.getValue().toString() + "\t";
//				        }
//
//				    FileWriter fw = new FileWriter(outputFile, true);    
//				    fw.write(temp + "\n"); 
//					fw.close();
//				} catch (IOException ioe) {
//					System.err.println("IOException: " + ioe.getMessage());
//				}
				

			}
			
			for (int index = 1; index <= 25; index++) {
				for (String str : OrderBy.get(index)) {

					try {
						FileWriter fw = new FileWriter(outputFile, true);
						fw.write(str + "\n");
						fw.close();
					} catch (IOException ioe) {
						System.err.println("IOException: " + ioe.getMessage());
					}

				}
			}
			
		}
		tempEstimatedTime = System.currentTimeMillis() - startTime;

		System.out.println("Execution Time for Transfer and Print: " + tempEstimatedTime);
	
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
			boolean allowLargeResults) throws TimeoutException, InterruptedException {
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queryString)
				// Save the results of the query to a permanent table.
				// See:
				// https://cloud.google.com/bigquery/querying-data#permanent-table
				.setDestinationTable(TableId.of(destinationDataset, destinationTable))
				// Allow results larger than the maximum response size.
				// If true, a destination table must be set.
				// See:
				// https://cloud.google.com/bigquery/querying-data#large-results
				.setAllowLargeResults(allowLargeResults).build();

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

	public static String prepareAnnotateVariantQuery(String VCFTableNames, String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
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
		String VCFQuery=" ( SELECT * from ";
		for (int index=0; index< VCFTables.length; index++){
			if(VCFCanonicalize!=null)
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, END, reference_bases, alternate_bases ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END, reference_bases, alternate_bases ";

			VCFQuery += " FROM ["+ VCFTables[index]  +"] OMIT RECORD IF EVERY(call.genotype <= 0) ), ";
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
						
						for (int index2=2; index2<TableInfo.length; index2++){
							RequestedFields += AliasTableName +"." + TableInfo[index2] + " , ";
							if(index2==2){
								//AllFields += "CONCAT(\""+ TableInfo[1] +": \","+ AliasTableName +"." + TableInfo[index2] + " ) , ";
								AllFields += "CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[index2] + " ) , ";
							}
							else
								AllFields += AliasTableName +"." + TableInfo[index2] + " , ";

						}
												
						AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, " + RequestedFields
							+ " FROM " + VCFQuery 
							+ " JOIN [" + TableName +"] AS " + AliasTableName ;
						AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start = VCF.start) AND (" + AliasTableName + ".END = VCF.END) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alt) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alt = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".base))), ";
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
				
				for (int index2=2; index2<TableInfo.length; index2++){
					RequestedFields += AliasTableName +"." + TableInfo[index2] + " , ";
					if(index2==2)
						AllFields += "CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[index2] + " ) , ";
					else
						AllFields += AliasTableName +"." + TableInfo[index2] + " , ";
				}
				
				
				AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, " + RequestedFields
					+ " FROM " + VCFQuery 
					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";

				AnnotationQuery += " WHERE "
						+ "((("+ AliasTableName +".start >= VCF.start) AND ("+ AliasTableName +".start <= VCF.END)) OR "
						+ " (("+ AliasTableName +".END >= VCF.start) AND ("+ AliasTableName +".END <= VCF.END)) OR "
						+ " (("+ AliasTableName +".start <= VCF.start) AND ("+ AliasTableName +".END >= VCF.start)) OR "
						+ " (("+ AliasTableName +".start <= VCF.END) AND ("+ AliasTableName +".END >= VCF.END))) "
						+ "), ";				
			}
			
		}		
	
		String Query= "  SELECT "
				+ " VCF.reference_name as chrm, VCF.start as start, VCF.END as end, VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, " + AllFields 
				+ " FROM ";
		
		Query += AnnotationQuery;
		//Query += " group by chrm,  start,  end, reference_bases, alternate_bases, " + GroupByFields; 

		
		return Query;
	}
	
	
	String prepareQueryGeneBased(){
/*
		SELECT
		  count (*)
		FROM (
		  SELECT
		    REPLACE(reference_name, 'chr', '') AS reference_name,
		    start,
		    END,
		    reference_bases,
		    alternate_bases
		  FROM
		    [genomics-public-data:platinum_genomes.variants]
		  OMIT
		    RECORD IF EVERY(call.genotype <= 0) ) AS VCF
		LEFT JOIN
		  [gbsc-gcp-project-cba:annotation.refGene17] AS VCF2
		ON
		  VCF.reference_name=VCF2.chrm
		  AND VCF.start=VCF2.start
		  AND VCF.END=VCF2.END
		WHERE
		  VCF2.chrm IS NULL;
*/		
		String test="";
		return test;
		
	}
	
	
}
