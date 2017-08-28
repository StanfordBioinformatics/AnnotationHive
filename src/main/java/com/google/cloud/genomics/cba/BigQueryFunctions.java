package com.google.cloud.genomics.cba;

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

public class BigQueryFunctions {
	public static boolean DEBUG = false;

	public static void sort(String projectId, String datasetId, String tableId, String outputFile)
			throws Exception {

		String queryStat = "SELECT *, LENGTH(innerQ.Chromosome) as len from (SELECT  RIGHT(chrm, LENGTH(chrm) - 3) as Chromosome, "
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
					sortAndPrint("chr" + row.get(0).getValue().toString(),
							Integer.parseInt(row.get(1).getValue().toString()),
							Integer.parseInt(row.get(2).getValue().toString()),
							Integer.parseInt(row.get(3).getValue().toString()), projectId, datasetId, tableId,
							outputFile);
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
					projectId, datasetId, tableId, outputFile);

		}
		if (Y != null) {
			System.out.println(Y.get(0).getValue().toString() + " " + Y.get(1).getValue().toString() + " "
					+ Y.get(2).getValue().toString() + " " + Y.get(3).getValue().toString());
			sortAndPrint("chr" + Y.get(0).getValue().toString(), Integer.parseInt(Y.get(1).getValue().toString()),
					Integer.parseInt(Y.get(2).getValue().toString()), Integer.parseInt(Y.get(3).getValue().toString()),
					projectId, datasetId, tableId, outputFile);

		}
		if (M != null) {
			System.out.println(M.get(0).getValue().toString() + " " + M.get(1).getValue().toString() + " "
					+ M.get(2).getValue().toString() + " " + M.get(3).getValue().toString());
			sortAndPrint("chr" + M.get(0).getValue().toString(), Integer.parseInt(M.get(1).getValue().toString()),
					Integer.parseInt(M.get(2).getValue().toString()), Integer.parseInt(M.get(3).getValue().toString()),
					projectId, datasetId, tableId, outputFile);

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

		String Query = "SELECT info FROM (SELECT  * FROM ";

		
	
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

		Query += " order by chrm) ";
		
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

	
	private static void sortAndPrint(String chromId, int count, int startMin, int startMax, String projectId,
			String datasetId, String tableId, String outputFile) {

		// for (int interval = startMin; interval<= startMax; interval += 100000
		// ){
		String queryStat = "SELECT  * from " + "[" + projectId + ":" + datasetId + "." + tableId + "] "
				+ "where chrm='" + chromId + "'"// and "
				// + Integer.toString(interval) + "<= start and start <= " +
				// Integer.toString(interval+100000)
				+ " order by chrm, start";
		//if (DEBUG)
			System.out.println(queryStat);
		// Get the results.
		QueryResponse response = runquery(queryStat);
		QueryResult result = response.getResult();
		if (result != null) {
			for (List<FieldValue> row : result.iterateAll()) {
				try {
					 String temp="";
				      for (FieldValue val : row) {
				          //System.out.printf("%s,", val.toString());
				    	  temp += val.getValue().toString() + "\t";
				        }
				        //System.out.printf("\n");
				    FileWriter fw = new FileWriter(outputFile, true);    
				    //fw.write(row.get(0).getValue().toString() + "\n"); 
				    fw.write(temp + "\n"); 
					fw.close();
				} catch (IOException ioe) {
					System.err.println("IOException: " + ioe.getMessage());
				}

			}
		}
		// }//END of FOR LOOP
	}

	private static QueryResponse runquery(String querySql) {

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

	public static String prepareAnnotateVariantQuery(String VCFTableName, String transcriptAnnotationTableNames,
			String variantAnnotationTableNames) {

		String Query= "  SELECT "
				+ " VCF.reference_name as chrm, VCF.start as start, VCF.END as end, VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, Annotation.info"
				+ " FROM ";
		
		String[] transcriptAnnotations=null;
		String[] variantAnnotationTables=null;
		
		if(!transcriptAnnotationTableNames.isEmpty()){
			transcriptAnnotations = transcriptAnnotationTableNames.split(","); 
		}
	
		if(variantAnnotationTableNames!=null){
			variantAnnotationTables = variantAnnotationTableNames.split(","); 
		}
			
		if(variantAnnotationTables!=null){
			boolean prefix_chr=false; //For 1000 genomes (chrom=1) = false; for Plantnium is yes (chrom=chr1)
			for (String annotationTable:variantAnnotationTables){
				if(prefix_chr){
					Query += " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, Annotation.info "
						+ " FROM ["+ VCFTableName  +"] AS VCF "
						+ " JOIN [" + annotationTable +"] AS Annotation "
						+ " ON (Annotation.chrm = VCF.reference_name) "
						+ " AND (Annotation.start = VCF.start) AND (Annotation.END = VCF.END) "
						+ " WHERE (((CONCAT(VCF.reference_bases, Annotation.alt) = VCF.alternate_bases) "
						+ " OR Annotation.alt = VCF.alternate_bases) AND (VCF.reference_bases = Annotation.base))), ";
				}else{	
					
					Query += " ( SELECT VCF.reference_name, VCF.start, VCF.END, "
						+ " VCF.reference_bases, VCF.alternate_bases, Annotation.info "
						+ " FROM (SELECT concat('chr', reference_name) as reference_name, start, END, "
						+ "reference_bases, alternate_bases from ["+ VCFTableName +"]) AS VCF "
						+ " JOIN [" + annotationTable +"] AS Annotation "
						+ " ON (Annotation.chrm = VCF.reference_name) "
						+ " AND (Annotation.start = VCF.start) AND (Annotation.END = VCF.END) "
						+ " WHERE (((CONCAT(VCF.reference_bases, Annotation.alt) = VCF.alternate_bases) "
						+ " OR Annotation.alt = VCF.alternate_bases) AND (VCF.reference_bases = Annotation.base))), ";
					
				}
						
			}
						
		}
		
		if(transcriptAnnotations!=null){
		
		}		
		
//		String s ="  SELECT "
//		+ " VCF.reference_name as chrm, VCF.start as start, VCF.END as end, VCF.reference_bases as reference_bases, VCF.alternate_bases as alternate_bases, Annotation.info"
//		+ " FROM "
//		
//		+ " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, Annotation.info "
//		+ " FROM [genomics-public-data:platinum_genomes.variants] AS VCF "
//		+ " JOIN [gbsc-gcp-project-cba:annotation.Cosmic68] AS Annotation "
//		+ " ON (Annotation.chrm = VCF.reference_name) "
//		+ " AND (Annotation.start = VCF.start) AND (Annotation.END = VCF.END) "
//		+ " WHERE (((CONCAT(VCF.reference_bases, Annotation.alt) = VCF.alternate_bases) "
//		+ " OR Annotation.alt = VCF.alternate_bases) AND (VCF.reference_bases = Annotation.base)) ) AS A1, "
//		
//			
//		+ " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, Annotation.info "
//		+ " FROM [genomics-public-data:platinum_genomes.variants] AS VCF "
//		+ " JOIN [gbsc-gcp-project-cba:annotation.Cosmic68_2] AS Annotation "
//		+ " ON (Annotation.chrm = VCF.reference_name) "
//		+ " AND (Annotation.start = VCF.start) AND (Annotation.END = VCF.END) "
//		+ " WHERE (((CONCAT(VCF.reference_bases, Annotation.alt) = VCF.alternate_bases) "
//		+ " OR Annotation.alt = VCF.alternate_bases) AND (VCF.reference_bases = Annotation.base)) ) AS A2, "
//		
//
//		+ " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, Annotation.info "
//		+ " FROM [genomics-public-data:platinum_genomes.variants] AS VCF "
//		+ " JOIN [gbsc-gcp-project-cba:annotation.Cosmic68_3] AS Annotation "
//		+ " ON (Annotation.chrm = VCF.reference_name) "
//		+ " AND (Annotation.start = VCF.start) AND (Annotation.END = VCF.END) "
//		+ " WHERE (((CONCAT(VCF.reference_bases, Annotation.alt) = VCF.alternate_bases) "
//		+ " OR Annotation.alt = VCF.alternate_bases) AND (VCF.reference_bases = Annotation.base)) ) AS A3, "
//		
//		+ " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, Annotation.info "
//		+ " FROM [genomics-public-data:platinum_genomes.variants] AS VCF "
//		+ " JOIN [gbsc-gcp-project-cba:annotation.Cosmic68_4] AS Annotation "
//		+ " ON (Annotation.chrm = VCF.reference_name) "
//		+ " AND (Annotation.start = VCF.start) AND (Annotation.END = VCF.END) "
//		+ " WHERE (((CONCAT(VCF.reference_bases, Annotation.alt) = VCF.alternate_bases) "
//		+ " OR Annotation.alt = VCF.alternate_bases) AND (VCF.reference_bases = Annotation.base)) ) AS A4, "
//		
//		+ " ( SELECT VCF.reference_name, VCF.start, VCF.END, VCF.reference_bases, VCF.alternate_bases, Annotation.info "
//		+ " FROM [genomics-public-data:platinum_genomes.variants] AS VCF "
//		+ " JOIN [gbsc-gcp-project-cba:annotation.Cosmic68_5] AS Annotation "
//		+ " ON (Annotation.chrm = VCF.reference_name) "
//		+ " AND (Annotation.start = VCF.start) AND (Annotation.END = VCF.END) "
//		+ " WHERE (((CONCAT(VCF.reference_bases, Annotation.alt) = VCF.alternate_bases) "
//		+ " OR Annotation.alt = VCF.alternate_bases) AND (VCF.reference_bases = Annotation.base)) ) AS A5 "
//		
//		+ "group by chrm,  start,  end, reference_bases, alternate_bases, Annotation.info";
		
		
		Query += " group by chrm,  start,  end, reference_bases, alternate_bases, Annotation.info ";

		
		return Query;
	}
	
}
