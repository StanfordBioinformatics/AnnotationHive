package com.google.cloud.genomics.cba;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Main Class: Start Annotation Engine!
 */

public class StartAnnotationEngine {

	public static void main(String[] args) throws IOException {

		if (args == null || args.length == 0){
			throw new IllegalArgumentException("Please select a proper method (ImportAnnotation/AnnotateVariants/UploadFile/ImportVCF/ExportVCF/BigQueryImportAnnotation/BigQueryAnnotateVariants)");	
		}

		try {
			if(args[0].equalsIgnoreCase("AnnotateVariants")){
				AnnotateVariants.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if(args[0].equalsIgnoreCase("ImportAnnotations")){
				ImportAnnotation.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if (args[0].equalsIgnoreCase("ImportVCF")){
				ImportVCF.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if (args[0].equalsIgnoreCase("ExportVCF")){
				ExportVCF.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if (args[0].equalsIgnoreCase("UploadFile")){
				CloudStorage.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if (args[0].equalsIgnoreCase("BigQueryImportAnnotation")){
				BigQueryImportAnnotation.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if (args[0].equalsIgnoreCase("BigQueryAnnotateVariants")){
				BigQueryAnnotateVariants.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else
				throw new IllegalArgumentException("Please select a proper method (ImportAnnotation/AnnotateVariants/UploadFile/ImportVCF/ExportVCF/BigQueryImportAnnotation/BigQueryAnnotateVariants)");	
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
}


