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

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class BigQueryFunctions  {
	public static boolean DEBUG = false;

	public static void runQuery(String projectId, String datasetId, String tableId, String outputFile) throws Exception{
	
		
		String queryStat = "SELECT *, LENGTH(innerQ.Chromosome) as len from (SELECT  RIGHT(chrm, LENGTH(chrm) - 3) as Chromosome, "
		+ "count(*) as Cnt, min (start) as Minimum, max(start) as Maximum FROM   "
		+ "[" + projectId +":" + datasetId +"." + tableId + "] "
		+ "group by Chromosome  order by Chromosome) as innerQ  order by len, innerQ.Chromosome";
		// Get the results.
		QueryResponse response = runquery(queryStat); 
		QueryResult result = response.getResult();
		
		// Per each chromosm, sort the result.
		List<FieldValue> X=null, Y=null, M=null;
		while (result != null) {
		   for (List<FieldValue> row : result.iterateAll()) {
		    		String chrm = row.get(0).getValue().toString();
		    	  if (!chrm.equals("M") && !chrm.equals("X")
		    			  && !chrm.equals("Y"))
		    	  {
		    		  System.out.println(chrm + " " +  row.get(1).getValue().toString() 
		    				  + " " +  row.get(2).getValue().toString()
		    				  + " " +  row.get(3).getValue().toString()
		    				  );
		    	  	  sortAndPrint("chr" + row.get(0).getValue().toString(), Integer.parseInt(row.get(1).getValue().toString()), 
		    	  			Integer.parseInt(row.get(2).getValue().toString()), Integer.parseInt(row.get(3).getValue().toString()),
		    	  			projectId, datasetId, tableId, outputFile);
		    	  }
		    	  else{
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
		if(X!=null){
  		  System.out.println(X.get(0).getValue().toString() + " " 
  				  +  X.get(1).getValue().toString() 
				  + " " +  X.get(2).getValue().toString()
				  + " " +  X.get(3).getValue().toString()
				  );

  		  sortAndPrint("chr" + X.get(0).getValue().toString(), Integer.parseInt(X.get(1).getValue().toString()), 
	  			Integer.parseInt(X.get(2).getValue().toString()), Integer.parseInt(X.get(3).getValue().toString()),
	  			projectId, datasetId, tableId, outputFile);

		}
		if(Y!=null){
	  		  System.out.println(Y.get(0).getValue().toString() + " " 
	  				  +  Y.get(1).getValue().toString() 
					  + " " +  Y.get(2).getValue().toString()
					  + " " +  Y.get(3).getValue().toString()
					  );
    	  	  sortAndPrint("chr" + Y.get(0).getValue().toString(), Integer.parseInt(Y.get(1).getValue().toString()), 
    	  			Integer.parseInt(Y.get(2).getValue().toString()), Integer.parseInt(Y.get(3).getValue().toString()),
    	  			projectId, datasetId, tableId, outputFile);

		}
		if(M!=null)
		{
	  		  System.out.println(M.get(0).getValue().toString() + " " +  
	  				  M.get(1).getValue().toString() 
					  + " " +  M.get(2).getValue().toString()
					  + " " +  M.get(3).getValue().toString()
					  );
    	  	  sortAndPrint("chr" + M.get(0).getValue().toString(), Integer.parseInt(M.get(1).getValue().toString()), 
    	  			Integer.parseInt(M.get(2).getValue().toString()), Integer.parseInt(M.get(3).getValue().toString()),
    	  			projectId, datasetId, tableId, outputFile);

		}
	}

	private static void sortAndPrint(String chromId, int count, int startMin, int startMax, 
			String projectId, String datasetId, String tableId, String outputFile) {

		//for (int interval = startMin; interval<= startMax; interval += 100000 ){
		String queryStat = "SELECT  info from "
				+ "[" + projectId +":" + datasetId +"." + tableId + "] "
				+ "where chrm='" +  chromId + "'"// and "
				//+ Integer.toString(interval) + "<= start and  start <= " + Integer.toString(interval+100000)
				+ " order by chrm, start";
		if (DEBUG)
			System.out.println(queryStat);
		// Get the results.
		QueryResponse response = runquery(queryStat); 
		QueryResult result = response.getResult();
				if (result != null) {
					for (List<FieldValue> row : result.iterateAll()) {
						try
						{
						    FileWriter fw = new FileWriter(outputFile, true); //the true will append the new data
						    fw.write(row.get(0).getValue().toString()+"\n"); //("add a line\n");//appends the string to the file
						    fw.close();
						}
						catch(IOException ioe)
						{
						    System.err.println("IOException: " + ioe.getMessage());
						}
												
					}	  
				}
		//}//END of FOR LOOP	
	}

	private static QueryResponse runquery(String querySql) {

		QueryJobConfiguration queryConfig =
				QueryJobConfiguration.newBuilder(querySql).build();
	
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
			// You can also look at queryJob.getStatus().getExecutionErrors() for all
			// errors, not just the latest one.
			throw new RuntimeException(queryJob.getStatus().getError().toString());
		}
		
		return bigquery.getQueryResults(jobId);
	}	
}
