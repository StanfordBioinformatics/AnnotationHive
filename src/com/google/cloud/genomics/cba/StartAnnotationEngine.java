package com.google.cloud.genomics.cba;


import java.io.IOException;
import java.util.logging.Logger;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;

import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.cloud.genomics.cba.httpRequests.Dataflow;

/**
 * Main Class: Start Annotation Engine!
 */

public class StartAnnotationEngine {

	public static final Logger LOG = Logger.getLogger(Dataflow.class.getName());
	static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	static final JsonFactory JSON_FACTORY = new JacksonFactory();

	public static void main(String[] args) throws IOException {

		
		try {
			AnnotateVariants.run(args);
			//ImportAnnotation.run(args);
			//ImportVCF.run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		if (args.length != 6) {
//	      System.err.println("Usage:");
//      	  System.err.println("\t <Cloud Project ID> <ReferenceSetId> <DatasetId> "
//      			+ "<Google Dataflow Staging Path (e.g., gs://myBucketName/staging)> "
//      	  		+ "<Address of Input Object (e.g., gs://myBucketName"
//      	  		+ "/myObject.txt)> <Address of Output Object>");
//	      System.exit(1);
//	    } 
//		
//	
//		
//		try {
//			
//			String projectId = args[0];
//			String referenceSetId = args[1];
//			String datasetId = args [2];
//			String stagingPath = args[3];
//			String inputObjectAddr= args[4];
//			String outputObjectAddr= args[5];
//			
//			System.out.println("Project ID: " +  projectId);
//			System.out.println("Reference Set ID: " +  referenceSetId);
//			System.out.println("Dataset ID: " +  datasetId );
//			System.out.println("Staging Path: " +  stagingPath);
//			System.out.println("Address of Input Object: " +  inputObjectAddr);
//			System.out.println("Address of Output Object: " +  outputObjectAddr);
//	
//			//To-DO
//				// STEP 0: initialize annotation header => Matching Engine provides
//				// annotation header list
//
//				/*
//				 * Automatic Schema Mapping
//				 * 
//				 * annotationSetId referenceName start end info name referenceId
//				 * reverseStrand transcript variant
//				 * 
//				 */
//		
//			// STEP 1: authentication - login credentials
//			CurlHttpRequests.command("gcloud auth login");
//
//			// STEP 2: get the authentication TOKEN
//			final String TOKEN = CurlHttpRequests.command("echo $(gcloud auth print-access-token)");
//			System.out.println("Hello Authentication TOKEN is: " + TOKEN);
//
//			// STEP 3: create a new annotation set		
//			AnnotationSet newAnnotationSet = new AnnotationSet();
//			
//			newAnnotationSet.setAnnotaionSetName("");
//			newAnnotationSet.setReferenceSetId (referenceSetId); 
//			newAnnotationSet.setDatasetId(datasetId); 
//			newAnnotationSet.setAnnotationSetSourceUri("");
//			newAnnotationSet.setTOKEN(TOKEN);
//			newAnnotationSet.setAnnotationSetType("");
//
//			newAnnotationSet.submitAnnotationSet();
//		
//			final String annotationSetId = newAnnotationSet.getAnnotationSetId();
//		
//			System.out.println("AnnotationSet ID: " + annotationSetId);
//
//			if (annotationSetId == null) {
//				System.err.println("\n\n\t\tcreateAnnotationSet FAILED!");
//				System.exit(0);
//			}
//
//			// STEP 4: creating annotations using a Google Dataflow pipeline						
//			 Dataflow.runPipeline(TOKEN, annotationSetId, projectId, 
//					 stagingPath, inputObjectAddr, outputObjectAddr);
//
//
//			System.out.println("Done");
//
//		} catch (IllegalStateException e) {
//			System.err.println(e.getMessage());
//		} catch (Throwable t) {
//			t.printStackTrace();
//		}
	}

}

