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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;


public class BigQueryFunctions {
	private static final Logger LOG = Logger.getLogger(BigQueryAnnotateVariants.class.getName());

	public static boolean DEBUG = false;

//	public static void sort(String projectId, String datasetId, String tableId, String outputFile, int BinSize)
//			throws Exception {
//
////		String queryStat = "SELECT *, LENGTH(innerQ.Chromosome) as len from (SELECT  RIGHT(chrm, LENGTH(chrm) - 3) as Chromosome, "
////				+ "count(*) as Cnt, min (start) as Minimum, max(start) as Maximum FROM   " + "[" + projectId + ":"
////				+ datasetId + "." + tableId + "] "
////				+ "group by Chromosome  order by Chromosome) as innerQ  order by len, innerQ.Chromosome";
//
//		String queryStat = "SELECT *, LENGTH(innerQ.Chromosome) as len from (SELECT  chrm as Chromosome, "
//				+ "count(*) as Cnt, min (start) as Minimum, max(start) as Maximum FROM   " + "[" + projectId + ":"
//				+ datasetId + "." + tableId + "] "
//				+ "group by Chromosome  order by Chromosome) as innerQ  order by len, innerQ.Chromosome";
//
//		
//		// Get the results.
//		QueryResponse response = runquery(queryStat);
//		QueryResult result = response.getResult();
//
//		// Per each chromosome, sort the result.
//		List<FieldValue> X = null, Y = null, M = null;
//		while (result != null) {
//			for (List<FieldValue> row : result.iterateAll()) {
//				String chrm = row.get(0).getValue().toString();
//				if (!chrm.equals("M") && !chrm.equals("MT")  && !chrm.equals("X") && !chrm.equals("Y")) {
//					System.out.println(chrm + " " + row.get(1).getValue().toString() + " "
//							+ row.get(2).getValue().toString() + " " + row.get(3).getValue().toString());
//					sortAndPrint(row.get(0).getValue().toString(),
//							Integer.parseInt(row.get(1).getValue().toString()),
//							Integer.parseInt(row.get(2).getValue().toString()),
//							Integer.parseInt(row.get(3).getValue().toString()), projectId, datasetId, tableId,
//							outputFile, BinSize);
//				} else {
//					if (chrm.equals("M") || chrm.equals("MT"))
//						M = row;
//					else if (chrm.equals("X"))
//						X = row;
//					else if (chrm.equals("Y"))
//						Y = row;
//				}
//			}
//			result = result.getNextPage();
//		}
//		if (X != null) {
//			System.out.println(X.get(0).getValue().toString() + " " + X.get(1).getValue().toString() + " "
//					+ X.get(2).getValue().toString() + " " + X.get(3).getValue().toString());
//
//			sortAndPrint("chr" + X.get(0).getValue().toString(), Integer.parseInt(X.get(1).getValue().toString()),
//					Integer.parseInt(X.get(2).getValue().toString()), Integer.parseInt(X.get(3).getValue().toString()),
//					projectId, datasetId, tableId, outputFile, BinSize);
//
//		}
//		if (Y != null) {
//			System.out.println(Y.get(0).getValue().toString() + " " + Y.get(1).getValue().toString() + " "
//					+ Y.get(2).getValue().toString() + " " + Y.get(3).getValue().toString());
//			sortAndPrint("chr" + Y.get(0).getValue().toString(), Integer.parseInt(Y.get(1).getValue().toString()),
//					Integer.parseInt(Y.get(2).getValue().toString()), Integer.parseInt(Y.get(3).getValue().toString()),
//					projectId, datasetId, tableId, outputFile, BinSize);
//
//		}
//		if (M != null) {
//			System.out.println(M.get(0).getValue().toString() + " " + M.get(1).getValue().toString() + " "
//					+ M.get(2).getValue().toString() + " " + M.get(3).getValue().toString());
//			sortAndPrint("chr" + M.get(0).getValue().toString(), Integer.parseInt(M.get(1).getValue().toString()),
//					Integer.parseInt(M.get(2).getValue().toString()), Integer.parseInt(M.get(3).getValue().toString()),
//					projectId, datasetId, tableId, outputFile, BinSize);
//
//		}
//	}
//
	
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

	
	
	
	
//	public static void sortAllAndPrint(String projectId,
//			String datasetId, String tableId, String outputFile) {
//
//		String Query = "SELECT  * FROM ";
//
//		
//	
//		for (int chrmId=1; chrmId<23; chrmId++){
//			Query +=  " (SELECT CAST (RIGHT(chrm, LENGTH(chrm) - 3) as INTEGER) as chrm, CAST (start as INTEGER), end, "
//					+ "reference_bases, alternate_bases, Annotation_info FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
//					+ "where chrm='chr" + chrmId + "'"
//					+ " order by chrm, start) as chrm" + chrmId + ", ";
//
//		}
//		
//		
//		Query +=  " (SELECT CAST ('23' as INTEGER) as chrm, CAST (start as INTEGER), end, "
//					+ "reference_bases, alternate_bases, Annotation_info FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
//				+ "where chrm='chrX'"
//				+ " order by chrm, start) as chrmX, ";
//		
//		Query +=  " (SELECT CAST ('24' as INTEGER) as chrm, CAST (start as INTEGER), end, "
//					+ "reference_bases, alternate_bases, Annotation_info FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
//				+ "where chrm='chrY'"
//				+ " order by chrm, start) as chrmY, ";
//
//		Query +=  " (SELECT CAST ('25' as INTEGER) as chrm, CAST (start as INTEGER), end, "
//					+ "reference_bases, alternate_bases, Annotation_info FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
//				+ "where chrm='chrM'"
//				+ " order by chrm, start) as chrmM ";
//
//		Query += " order by chrm ";
//		
//		//START - Reset start for BigQuery 
//		long startTime = System.currentTimeMillis();
//				
//		
//		//if (DEBUG)
//			System.out.println(Query);
//		// Get the results.
//		QueryResponse response = runquery(Query);
//		QueryResult result = response.getResult();
//		
//		//END - RUN time 
//		long tempEstimatedTime = System.currentTimeMillis() - startTime;
//
//		System.out.println("Execution Time for Sort: " + tempEstimatedTime);
//		
//		//START - Reset start for BigQuery 
//		startTime = System.currentTimeMillis();
//
//		
//		if (result != null) {
//			for (List<FieldValue> row : result.iterateAll()) {
//				try {
//					 String temp="";
//				      for (FieldValue val : row) {
//				    	  temp += val.getValue().toString() + "\t";
//				        }
//				      if(temp.startsWith("23")){
//				    	  temp=temp.substring(2);
//				    	  temp = "X" + temp;
//				      }
//				      else if (temp.startsWith("24")){
//				    	  temp = temp.substring(2);
//				    	  temp = "Y" + temp;
//				      }
//				      else if (temp.startsWith("25")){
//				    	  temp = temp.substring(2);
//				    	  temp = "M" + temp;
//				      }
//
//				    FileWriter fw = new FileWriter(outputFile, true);    
//				    fw.write(temp + "\n"); 
//					fw.close();
//				} catch (IOException ioe) {
//					System.err.println("IOException: " + ioe.getMessage());
//				}
//				
//
//			}
//			
//		}
//		tempEstimatedTime = System.currentTimeMillis() - startTime;
//
//		System.out.println("Execution Time for Transfer and Print: " + tempEstimatedTime);
//	
//	}
	
	
//	public static void sortAllAndPrintDataflow(String projectId,
//			String datasetId, String tableId, String outputFile) {
//
//		String Query = "SELECT chrm, info FROM (SELECT  * FROM ";
//
//		
//	
//		for (int chrmId=1; chrmId<23; chrmId++){
//			Query +=  " (SELECT CAST (RIGHT(chrm, LENGTH(chrm) - 3) as INTEGER) as chrm, start, end, info "
//					+ " FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
//					+ "where chrm='chr" + chrmId + "'"
//					+ " order by chrm, start) as chrm" + chrmId + ", ";
//		}
//
//		Query +=  " (SELECT CAST ('23' as INTEGER) as chrm, start, end, info "
//					+ "FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
//				+ "where chrm='chrX'"
//				+ " order by chrm, start) as chrmX, ";
//		
//		Query +=  " (SELECT CAST ('24' as INTEGER) as chrm, start, end, info "
//					+ "FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
//				+ "where chrm='chrY'"
//				+ " order by chrm, start) as chrmY, ";
//
//		Query +=  " (SELECT CAST ('25' as INTEGER) as chrm, start, end, info "
//					+ "FROM [" + projectId + ":" + datasetId + "." + tableId + "] "
//				+ "where chrm='chrM'"
//				+ " order by chrm, start) as chrmM ";
//
//		//TODO: make sure "order by" scales 
//		Query += ") "; // " order by chrm, start) ";
//		
//		//START - Reset start for BigQuery 
//		long startTime = System.currentTimeMillis();
//				
//		
//		//if (DEBUG)
//			System.out.println(Query);
//		// Get the results.
//		QueryResponse response = runquery(Query);
//		QueryResult result = response.getResult();
//		
//		//END - RUN time 
//		long tempEstimatedTime = System.currentTimeMillis() - startTime;
//
//		System.out.println("Execution Time for Sort: " + tempEstimatedTime);
//		
//		//START - Reset start for BigQuery 
//		startTime = System.currentTimeMillis();
//
//		//ArrayList<Integer> arrli = new ArrayList<Integer>(n);
//		HashMap<Integer, List<String>> OrderBy = new HashMap<Integer, List<String>>();
//		for (int index=1; index<=25; index++){
//			List<String> L = new ArrayList<String>();
//			OrderBy.put(index, L);
//		}
//		
//		
//		if (result != null) {
//			for (List<FieldValue> row : result.iterateAll()) {
//				String str2=(String) row.get(1).getValue();
//				Integer chrId= Integer.parseInt((String) row.get(0).getValue());
//				//System.out.println("CHID:" +chrId + "  " +str2);
//				OrderBy.get(chrId).add(str2);
//
////				try {
////					 String temp="";
////				      for (FieldValue val : row) {
////				    	  temp += val.getValue().toString() + "\t";
////				        }
////
////				    FileWriter fw = new FileWriter(outputFile, true);    
////				    fw.write(temp + "\n"); 
////					fw.close();
////				} catch (IOException ioe) {
////					System.err.println("IOException: " + ioe.getMessage());
////				}
//				
//
//			}
//			
//			for (int index = 1; index <= 25; index++) {
//				for (String str : OrderBy.get(index)) {
//
//					try {
//						FileWriter fw = new FileWriter(outputFile, true);
//						fw.write(str + "\n");
//						fw.close();
//					} catch (IOException ioe) {
//						System.err.println("IOException: " + ioe.getMessage());
//					}
//
//				}
//			}
//			
//		}
//		tempEstimatedTime = System.currentTimeMillis() - startTime;
//
//		System.out.println("Execution Time for Transfer and Print: " + tempEstimatedTime);
//	
//	}

	
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
			boolean allowLargeResults, int MaximumBillingTier, boolean LegacySql ) throws TimeoutException, InterruptedException {
		QueryJobConfiguration queryConfig;
		
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
	

	public static String prepareAnnotateVariantQuery(String VCFTableNames, String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			  String TranscriptCanonicalizeRefNames, String VariantAnnotationTableNames, String VariantannotationCanonicalizerefNames, boolean GroupBy) {

				
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
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name, quality  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name, quality ";

			if (index+1<VCFTables.length)
				VCFQuery += " FROM FLATTEN(["+ VCFTables[index]  +"], call.call_set_name) OMIT RECORD IF EVERY(call.genotype <= 0) ), ";
			else
				VCFQuery += " FROM FLATTEN(["+ VCFTables[index]  +"], call.call_set_name) OMIT RECORD IF EVERY(call.genotype <= 0) ) ";
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
							RequestedFields +=  " , " + AliasTableName +"." + TableInfo[index2] ;
							if(index2==2){
								if (GroupBy)
									AllFields += ", MAX ( CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[index2] + " )) ";
								else
									AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[index2] + " ) ";

								//CONCAT("hg19_cosmic70: ",Annotation1.CosmicID )
							}
							else{
								if (GroupBy)
									AllFields +=  " , MAX ( " + AliasTableName +"." + TableInfo[index2] + ") ";
								else
									AllFields +=  " , " + AliasTableName +"." + TableInfo[index2];

							}
						}
												
						AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, COUNT(VCF.call.call_set_name) AS number_samples, VCF.quality " + RequestedFields
							+ " FROM " + VCFQuery 
							+ " JOIN [" + TableName +"] AS " + AliasTableName ;
						AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";

						AnnotationQuery += " AND (" + AliasTableName + ".start = VCF.start) AND (" + AliasTableName + ".END = VCF.END) "
							+ " WHERE (((CONCAT(VCF.reference_bases, " + AliasTableName + ".alt) = VCF.alternate_bases) "
							+ " OR " + AliasTableName + ".alt = VCF.alternate_bases) AND (VCF.reference_bases = " + AliasTableName + ".base)) "
									+ "GROUP BY VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, VCF.quality " + RequestedFields;
						if(index+1 <  VariantAnnotationTables.length || (TranscriptAnnotations!=null))
							AnnotationQuery +=  "), ";
						else
							AnnotationQuery +=  ") ";
			}
		}
		
		 
		if(TranscriptAnnotations!=null){
			for (int index=0; index< TranscriptAnnotations.length; index++, AnnotationIndex++){

//				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
//				 * ProjectId: gbsc-gcp-project-cba
//				 * DatasetId: PublicAnnotationSets
//				 * TableId: hg19_refGene_chr17
//				 * Features: exonCount:exonStarts:exonEnds:score
//				 */
//				String [] TableInfo = TranscriptAnnotations[index].split(":");
//				
//				String RequestedFields="";
//				String AliasTableName= "Annotation" + AnnotationIndex; 
//				String TableName = TableInfo[0]+":"+TableInfo[1];
//				
//				for (int index2=2; index2<TableInfo.length; index2++){
//					RequestedFields += " , " + AliasTableName +"." + TableInfo[index2];
//					if(index2==2)
//						AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[index2] + " ) ";
//					else
//						AllFields += " , " + AliasTableName +"." + TableInfo[index2] ;
//				}
//				
//				
//				AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, GROUP_CONCAT_UNQUOTED(VCF.call.call_set_name) AS sample_names," + RequestedFields
//					+ " FROM " + VCFQuery 
//					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
//				AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";
//
//				AnnotationQuery += " WHERE "
//						+ "((("+ AliasTableName +".start >= VCF.start) AND ("+ AliasTableName +".start <= VCF.END)) OR "
//						+ " (("+ AliasTableName +".END >= VCF.start) AND ("+ AliasTableName +".END <= VCF.END)) OR "
//						+ " (("+ AliasTableName +".start <= VCF.start) AND ("+ AliasTableName +".END >= VCF.start)) OR "
//						+ " (("+ AliasTableName +".start <= VCF.END) AND ("+ AliasTableName +".END >= VCF.END))) "
//						+ "), ";				
			}
			
		}		
	
		String Query= "  SELECT "
				+ " VCF.reference_name as chrm, VCF.start as start, VCF.END as end, VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, number_samples, quality " + AllFields 
				+ " FROM ";
		
		Query += AnnotationQuery;
		if(GroupBy)
			Query += " GROUP BY  chrm, start, END, reference_bases, alternate_bases, number_samples, quality "; 
		
		return Query;
	}
	
	

	
	public static String prepareAnnotateVariantQueryConcatFields_mVCF(String VCFTableNames, String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			  String TranscriptCanonicalizeRefNames, String VariantAnnotationTableNames, String VariantannotationCanonicalizerefNames, boolean GroupBy) {

				
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
		for (int index=0; index< VCFTables.length; index++){
			if(VCFCanonicalize!=null)
				VCFQuery += " ( SELECT REPLACE(reference_name, '"+ VCFCanonicalize[index] +"', '') as reference_name, start, END, reference_bases, alternate_bases, call.call_set_name, quality  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, END, reference_bases, alternate_bases ";

			if (index+1<VCFTables.length)
				VCFQuery += " FROM ["+ VCFTables[index]  +"] OMIT RECORD IF EVERY(call.genotype <= 0) ), ";
			else
				VCFQuery += " FROM ["+ VCFTables[index]  +"] OMIT RECORD IF EVERY(call.genotype <= 0) ) ";
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
									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
									//AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + AliasTableName + " ) ";
								}
								else{
									AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
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
							else
								AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";

						}
						else{
							if(VariantAnnotationTables!=null)
								AllFields += ", CONCAT(\""+ (index+1+VariantAnnotationTables.length) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
							else	
								AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";

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
	
		String Query= "  SELECT "
				+ " VCF.reference_name as chrm, VCF.start as start, VCF.END as end, VCF.reference_bases as reference_bases, "
				+ "VCF.alternate_bases as alternate_bases " + AllFields 
				+ " FROM ";
		
		Query += AnnotationQuery;
		if(GroupBy)
			Query += " GROUP BY  chrm, start, END, reference_bases, alternate_bases, number_samples, quality "; 
		
		return Query;
	}

	
	
	public static String prepareAnnotateVariantQueryWithSampleNames(String VCFTableNames, String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			  String TranscriptCanonicalizeRefNames, String VariantAnnotationTableNames, String VariantannotationCanonicalizerefNames, String SampleNames, boolean GroupBy) {

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
//			for (int index=0; index< TranscriptAnnotations.length; index++, AnnotationIndex++){
//
//				/* Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_refGene_chr17:exonCount:exonStarts:exonEnds:score
//				 * ProjectId: gbsc-gcp-project-cba
//				 * DatasetId: PublicAnnotationSets
//				 * TableId: hg19_refGene_chr17
//				 * Features: exonCount:exonStarts:exonEnds:score
//				 */
//				String [] TableInfo = TranscriptAnnotations[index].split(":");
//				
//				String RequestedFields="";
//				String AliasTableName= "Annotation" + AnnotationIndex; 
//				String TableName = TableInfo[0]+":"+TableInfo[1];
//				
//				for (int index2=2; index2<TableInfo.length; index2++){
//					RequestedFields += " , " + AliasTableName +"." + TableInfo[index2];
//					if(index2==2)
//						AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + TableInfo[index2] + " ) ";
//					else
//						AllFields += " , " + AliasTableName +"." + TableInfo[index2] ;
//				}
//				
//				
//				AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, GROUP_CONCAT_UNQUOTED(VCF.call.call_set_name) AS sample_names," + RequestedFields
//					+ " FROM " + VCFQuery 
//					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
//				AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";
//
//				AnnotationQuery += " WHERE "
//						+ "((("+ AliasTableName +".start >= VCF.start) AND ("+ AliasTableName +".start <= VCF.END)) OR "
//						+ " (("+ AliasTableName +".END >= VCF.start) AND ("+ AliasTableName +".END <= VCF.END)) OR "
//						+ " (("+ AliasTableName +".start <= VCF.start) AND ("+ AliasTableName +".END >= VCF.start)) OR "
//						+ " (("+ AliasTableName +".start <= VCF.END) AND ("+ AliasTableName +".END >= VCF.END))) "
//						+ "), ";				
//			}

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
										
				AnnotationQuery += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases " + RequestedFields
					+ " FROM " + VCFQuery 
					+ " JOIN [" + TableName +"] AS " + AliasTableName ;
				AnnotationQuery += " ON (" + AliasTableName + ".chrm = VCF.reference_name) ";

				AnnotationQuery += " WHERE "			
				//		x1 <= y2 && y1 <= x2
				+ "((("+ AliasTableName +".start <= VCF.end) AND ("+ AliasTableName +".end >= VCF.start)))";


//				+ "((("+ AliasTableName +".start >= VCF.start) AND ("+ AliasTableName +".start <= VCF.END)) OR "
//				+ " (("+ AliasTableName +".END >= VCF.start) AND ("+ AliasTableName +".END <= VCF.END)) OR "
//				+ " (("+ AliasTableName +".start <= VCF.start) AND ("+ AliasTableName +".END >= VCF.start)) OR "
//				+ " (("+ AliasTableName +".start <= VCF.END) AND ("+ AliasTableName +".END >= VCF.END))) ";

				
				if(index+1 <  TranscriptAnnotations.length)
					AnnotationQuery +=  "), ";
				else
					AnnotationQuery += ") ";
			}
		
		
		
		
		}		
	
		String Query= "  SELECT "
				+ " VCF.reference_name as chrm, VCF.start as start, VCF.END as end, "
				+ "VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases " + AllFields 
				+ " FROM ";
		
		Query += AnnotationQuery;
		if(GroupBy)
			Query += " GROUP BY  chrm, start, END, reference_bases, alternate_bases "; 
		
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


	public static String prepareGeneBasedQueryConcatFields_mVCF_Range_Min_StandardSQL(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			String TranscriptCanonicalizeRefNames, int Threashold, boolean onlyIntrogenic, boolean OutputASTable,
			boolean MIN) {

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
		String VCFQuery = "";
		for (int index = 0; index < VCFTables.length; index++) {
			if (VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '" + VCFCanonicalize[index]
						+ "', '') as reference_name, start, `END`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, `END`, reference_bases, alternate_bases ";

			if (index + 1 < VCFTables.length)
				VCFQuery += " FROM `" + VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]
						+ "`   WHERE EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt WHERE alt NOT IN (\"<NON_REF>\", \"<*>\")) ";
			else
				VCFQuery += " FROM `" + VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]
						+ "` WHERE EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt WHERE alt NOT IN (\"<NON_REF>\", \"<*>\")) ";
		}
		VCFQuery += " ) as VCF ";

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
				Query += " GROUP BY VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, alternate_bases, Status";
				
			//:TODO M and MT case
			
		
		return Query;
	}

	public static String prepareGeneBasedQueryConcatFields_Range_Min_StandardSQL(String VCFTableNames,
			String VCFCanonicalizeRefNames, String TranscriptAnnotationTableNames,
			String TranscriptCanonicalizeRefNames, String SampleId, int Threashold, boolean onlyIntrogenic,
			boolean OutputASTable, boolean MIN) {

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
		String VCFQuery = "";
		for (int index = 0; index < VCFTables.length; index++) {
			if (VCFCanonicalize != null)
				VCFQuery += " ( SELECT REPLACE( reference_name, '" + VCFCanonicalize[index]
						+ "', '') as reference_name, start, `END`, reference_bases, alternate_bases  ";
			else
				VCFQuery += " ( SELECT REPLACE(reference_name, '', '') as reference_name, start, `END`, reference_bases, alternate_bases ";

			if (index + 1 < VCFTables.length)
				VCFQuery += " FROM `" + VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]
						+ "` v, v.call  WHERE call.call_set_name= \"" + SampleId
						+ "\" AND EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt WHERE alt NOT IN (\"<NON_REF>\", \"<*>\")) ";
			else
				VCFQuery += " FROM `" + VCFTables[index].split(":")[0] + "." + VCFTables[index].split(":")[1]
						+ "` v, v.call  WHERE call.call_set_name=\"" + SampleId
						+ "\" AND EXISTS (SELECT alternate_bases FROM UNNEST(alternate_bases) alt WHERE alt NOT IN (\"<NON_REF>\", \"<*>\")) ";
		}
		VCFQuery += " ) as VCF ";

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



	public static void listAnnotationDatasets(String projectId, String datasetId, String tableId,
			String annotationDatasetBuild, boolean printAll, String searchKeyword) {
				
		String queryStat ="";
		
		String Fields=" AnnotationSetName, AnnotationSetType, AnnotationSetSize, Build, AnnotationSetFields ";
		if (printAll) {
			Fields=" * ";
		}
		
		if (!annotationDatasetBuild.isEmpty())
		{
			queryStat ="SELECT "+ Fields + " from [" + projectId + ":"
					+ datasetId + "." + tableId + "] "
					+ "WHERE Build=\"" + annotationDatasetBuild + "\"";
		}else if (!searchKeyword.isEmpty()) {
			queryStat ="SELECT "+ Fields + " from [" + projectId + ":"
					+ datasetId + "." + tableId + "] "
					+ "WHERE AnnotationSetName LIKE \"%" + searchKeyword + "%\"";
		}
		else {
			queryStat ="SELECT "+ Fields + "  from [" + projectId + ":"
					+ datasetId + "." + tableId + "] "
					+ " Order By Build";
		}
		
		// Get the results.
		System.out.println("Stat Query: " + queryStat);
		QueryResponse response = runquery(queryStat);
		QueryResult result = response.getResult();
		LOG.info("");
		LOG.info("");
		LOG.info("<============ AnnotationList ============>");
		while (result != null) {
			for (List<FieldValue> row : result.iterateAll()) {
			    Integer columnsCount = row.size();
			    StringBuilder sb = new StringBuilder();
			    for(int i = 0; i < columnsCount; i++){
			    		if(i+1<columnsCount)
			    			sb.append(row.get(i).getStringValue() + " | ");
			    		else
			    			sb.append(row.get(i).getStringValue());

			    }
			    System.out.println(sb.toString());
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
							
							if(fieldIndex==2){
	
								if (TableInfo.length>3){ //Top Select 
									//(e.g.,     CONCAT(Annotation3.name, "/",Annotation3.name2) AS Annotation3.Annotation3)
									//AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + AliasTableName + " ) ";
									AllFields += "," + AliasTableName +"." + AliasTableName + " ";
									//AllFields += ", CONCAT(\""+ TableInfo[1].split("\\.")[1] +": \","+ AliasTableName +"." + AliasTableName + " ) ";
								}
								else{
									//AllFields += ", CONCAT(\""+ (index+1) +": \","+ AliasTableName +"." + TableInfo[fieldIndex] + " ) ";
									AllFields += ","+ AliasTableName +"." + TableInfo[fieldIndex] + " ";

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
				+ chrm + " as chrm, "+ start + " as start, "+ end + " as end, " + RefBases + " as reference_bases, "
				+   AltBase + " as alternate_bases " + AllFields 
				+ " FROM " + AnnotationQuery;
					
		Query += " GROUP BY  chrm, start, END, reference_bases, alternate_bases " + AllFields; 
	
		
		return Query;
	}

}
