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
 
import java.io.IOException;
import java.util.Arrays;

/**
 * Main Class: Start Annotation Engine!
 */

public class StartAnnotationHiveEngine {

	public static void main(String[] args) throws IOException {

		if (args == null || args.length == 0){
			throw new IllegalArgumentException(
					"\n\n##################AnnotationHive#####################\n"
					+"Please select a proper method: \n"
					+ "\t\tUploadFileToGCS\n"
					+ "\t\tImportAnnotationFromGCSToGG\n"
					+ "\t\tImportVCFFromGCSToGG\n"
					+ "\t\tGGAnnotateVariants\n"
					+ "\t\tExportVCFFromGGToBigQuery\n"
					+ "\t\tImportAnnotationFromGCSToBigQuery\n"
					+ "\t\tBigQueryAnnotateVariants\n"
					+ "##################AnnotationHive#####################\n\n");	
		}

		try{
			if(args[0].equalsIgnoreCase("GGAnnotateVariants")){
				GGAnnotateVariants.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if(args[0].equalsIgnoreCase("ImportAnnotationFromGCSToGG")){
				ImportAnnotationFromGCSToGG.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if (args[0].equalsIgnoreCase("ImportVCFFromGCSToGG")){
				ImportVCFFromGCSToGG.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if (args[0].equalsIgnoreCase("ExportVCFFromGGToBigQuery")){
				ExportVCFFromGGToBigQuery.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else if (args[0].equalsIgnoreCase("UploadFileToGCS")){
				CloudStorage.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else 	if (args[0].equalsIgnoreCase("ImportAnnotationFromGCSToBigQuery")){
				ImportAnnotationFromGCSToBigQuery.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else 
				if (args[0].equalsIgnoreCase("BigQueryAnnotateVariants")){
				BigQueryAnnotateVariants.run(Arrays.copyOfRange(args, 1, args.length));
			}
			else
			throw new IllegalArgumentException(
					"\n\n##################AnnotationHive#####################\n"
					+"Please select a proper method: \n"
					+ "\t\tUploadFileToGCS\n"
					+ "\t\tImportAnnotationFromGCSToGG\n"
					+ "\t\tImportVCFFromGCSToGG\n"
					+ "\t\tGGAnnotateVariants\n"
					+ "\t\tExportVCFFromGGToBigQuery\n"
					+ "\t\tImportAnnotationFromGCSToBigQuery\n"
					+ "\t\tBigQueryAnnotateVariants\n"
					+ "##################AnnotationHive#####################\n\n");	
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
}


