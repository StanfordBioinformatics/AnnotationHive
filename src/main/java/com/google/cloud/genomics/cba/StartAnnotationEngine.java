package com.google.cloud.genomics.cba;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


import javafx.util.Pair;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;


/**
 * Main Class
 */

public class StartAnnotationEngine {

	public static final Logger LOG = Logger.getLogger(Dataflow.class.getName());
	private static final String API_KEY = "";
	static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	static final JsonFactory JSON_FACTORY = new JacksonFactory();

	public static void main(String[] args) throws IOException {
		
		if (args.length != 5) {
	      System.err.println("Usage:");
      	  System.err.println("\t <Cloud Project ID> <ReferenceSetId> <DatasetId> <Input_bucket_name and input_filename> <Input_bucket_name and output_filename>");
	      System.exit(1);
	    } 


		try {

			// STEP 0: initialize annotation header => Matching Engine provides
			// annotation header list

			/*
			 * Automatic Mapping
			 * 
			 * annotationSetId referenceName start end info name referenceId
			 * reverseStrand transcript variant
			 * 
			 */
		
			// STEP 1: authentication - login credentials
			CurlHttpRequests.command("gcloud auth login");

			// STEP 2: get the authentication TOKEN
			final String TOKEN = CurlHttpRequests.command("echo $(gcloud auth print-access-token)");
			System.out.println("Hello Authentication TOKEN is: " + TOKEN);

			// STEP 3: create a new annotation set		
			AnnotationSet newAnnotationSet = new AnnotationSet();
			
			newAnnotationSet.setAnnotaionSetName("");
			newAnnotationSet.setReferenceSetId (args[0]); 
			newAnnotationSet.setDatasetId(args[1]); 
			newAnnotationSet.setAnnotationSetSourceUri("");
			newAnnotationSet.setTOKEN(TOKEN);
			newAnnotationSet.setAnnotationSetType("");

			newAnnotationSet.submitAnnotationSet();
		
			final String annotationSetId = newAnnotationSet.getAnnotationSetId();
		
			System.out.println("AnnotationSet ID: " + annotationSetId);

			if (annotationSetId == null) {
				System.err.println("\n\n\t\tcreateAnnotationSet FAILED!");
				System.exit(0);
			}

			// STEP 4: creating annotations using a Google Dataflow pipeline
			
			/*My cloud project*/
//			 Dataflow.runPipeline(TOKEN, annotationSetId, "midyear-pattern-133519", 
//					 "gs://cloud-annotation", //storage path 
//					 "gs://cloud-annotation/hg19_EUR.sites.2015_08.txt", // Bucket address of the input Text file  
//					 "gs://cloud-annotation/hg19_EUR.sites.2015_08.json"); // Bucket address of the output JSON file
			
			 Dataflow.runPipeline(TOKEN, annotationSetId, "gbsc-gcp-project-cba", 
			 "gs://gbsc-gcp-project-cba-user-abahman",		 
			 "gs://gbsc-gcp-project-cba-user-abahman/annotation/hg19_EUR.sites.2015_08.txt", // Bucket address of the input Text file  
			 "gs://gbsc-gcp-project-cba-user-abahman/annotation/hg19_EUR.sites.2015_08.json"); // Bucket address of the output JSON file			
			

//			// STEP 4: creating annotations (Submitting one annotation or a batch of annotations)
//
//			 // The name of the file to open.
//			 String fileName = "hg19_EUR.sites.2015_08.txt";
//			
//			 // This will reference one line at a time
//			 String line = null;
//
//			try {
//				// FileReader reads text files in the default encoding.
//				FileReader fileReader = new FileReader(fileName);
//
//				// Always wrap FileReader in BufferedReader.
//				BufferedReader bufferedReader = new BufferedReader(fileReader);
//				int index = 0;
//				int indexName = 0;
//				JsonArray datasets = new JsonArray();
//
//				while ((line = bufferedReader.readLine()) != null) {
//					String[] words = line.split("\t");
//					//indexName++;
//					/* line by line annotation */
//					HG19_EUR.createdbSNPAnnotation(TOKEN, annotationSetId, words); //, indexName);
//					HG19_EUR.getAnnotation(TOKEN, annotationSetId, words);
//
//					/* batch annotation */
////					JsonObject newAnnotation = HG19_EUR.createAnnotationJSON(annotationSetId, words);
////					datasets.add(newAnnotation);
////
////					index++;
////					if (index % 10 == 0) {
////						JsonObject annotations = new JsonObject();
////						annotations.add("annotations", datasets);
////						System.out.println(annotations.toString());
////						datasets = new JsonArray();
////						Annotation.batchCreateAnnotation(TOKEN, annotations.toString());
////					}
//			 }
//
//				// Always close files.
//				bufferedReader.close();
//			} catch (FileNotFoundException ex) {
//				System.out.println("Unable to open file '" + fileName + "'");
//			} catch (IOException ex) {
//				ex.printStackTrace();
//			}

			//CloudStorage.runUpload("abahman@stanford.edu", "gbsc-gcp-project-cba-user-abahman", "pom.xml" , "pom2.xml" );
			//CloudStorage.runDownload("abahman@stanford.edu", "cloud-annotation", "cloud_pom.xml", "cloud_pom.xml");
			System.out.println("Done");

		} catch (IllegalStateException e) {
			System.err.println(e.getMessage());
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

}

