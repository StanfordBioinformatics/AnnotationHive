package com.google.cloud.genomics.cba;

import java.io.IOException;
import java.util.Arrays;
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
			if(args[0].equalsIgnoreCase("AnnotateVariant")){
				AnnotateVariants.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if(args[0].equalsIgnoreCase("ImportAnnotation")){
				ImportAnnotation.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if (args[0].equalsIgnoreCase("SubmitVCF"))
				ImportVCF.run(Arrays.copyOfRange(args, 1, args.length));
			else
				System.out.println("Please select a proper method (ImportAnnotation/AnnotateVariant/SubmitVCF)"); 
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
}


