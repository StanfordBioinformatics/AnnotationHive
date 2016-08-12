package com.google.cloud.genomics.cba;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;
import com.google.api.services.dataflow.Dataflow;

/**
 * Main Class: Start Annotation Engine!
 */

public class StartAnnotationEngine {

	public static final Logger LOG = Logger.getLogger(Dataflow.class.getName());
//	static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
//	static final JsonFactory JSON_FACTORY = new JacksonFactory();

	public static void main(String[] args) throws IOException {

		if (args == null || args.length == 0){
			throw new IllegalArgumentException("Please select a proper method (ImportAnnotation/AnnotateVariant/UploadFile/ImportVCF)");	
		}

		try {
			if(args[0].equalsIgnoreCase("AnnotateVariant")){
				AnnotateVariants.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if(args[0].equalsIgnoreCase("ImportAnnotation")){
				ImportAnnotation.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if (args[0].equalsIgnoreCase("ImportVCF")){
				ImportVCF.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if (args[0].equalsIgnoreCase("UploadFile")){
				CloudStorage.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else
				throw new IllegalArgumentException("Please select a proper method (ImportAnnotation/AnnotateVariant/UploadFile/ImportVCF)");	
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
}


