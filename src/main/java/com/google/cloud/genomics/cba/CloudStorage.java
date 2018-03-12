package com.google.cloud.genomics.cba;

/*
 * Copyright (C) 2016-2018 Stanford University.
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

/**
 * Uploading and Downloading files - Cloud Storage
 * Modified version - Google Class - Handles uploads and downloads 
 * 
 */
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.util.Strings;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.ObjectAccessControl;
import com.google.api.services.storage.model.StorageObject;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;



import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;

public class CloudStorage {

	private static final boolean IS_APP_ENGINE = false;

	private static Options options;

	public static interface Options extends GenomicsOptions 
	{
	

		@Description("This provides bucketName. This is a required field.")
		@Default.String("")
		String getBucketName();
		void setbucketName(String bucketName);
		
		@Description("This provides the name of file on the cloud. This is a required field.")
		@Default.String("")
		String getCloudObjectName();
		void setCloudObjectName(String cloudObjectName);

		@Description("This provides username. This is a required field.")
		@Default.String("")
		String getUsername();
		void setUsername(String username);

		@Description("This provides contentType. This is a required field.")
		@Default.String("text/plain")
		String getContentType();
		void setContentType(String contentType);

		@Description("This provides LocalFilenameAddr. This is a required field.")
		@Default.String("")
		String getLocalFilenameAddr();
		void setLocalFilenameAddr(String localFilenameAddr);
		
	}
	
	
	public static void run(String[] args) throws GeneralSecurityException, IOException {

		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		if (options.getUsername().isEmpty()) {
			throw new IllegalArgumentException("username must be specified");
		}
		
		if (options.getBucketName().isEmpty()) {
			throw new IllegalArgumentException("bucketName must be specified");
		}
		
		if (options.getLocalFilenameAddr().isEmpty()) {
			throw new IllegalArgumentException("localFilenameAddr must be specified");
		}
		
		if (options.getCloudObjectName().isEmpty()) {
			throw new IllegalArgumentException("cloudObjectName must be specified (i.e., the name of the file on the cloud)");
		}
				
		try{
			runUpload(options.getUsername(), options.getBucketName(), options.getLocalFilenameAddr(), 
					options.getCloudObjectName());
		}
		catch (Exception e){
			throw e;
		}
		
//		System.out.println("");
//		System.out.println("");
//		System.out.println("[INFO] ------------------------------------------------------------------------");
//		System.out.println("[INFO] To check the current status of your job, use the following command:");
//		System.out.println("\t ~: gcloud alpha genomics operations describe $YOUR_OPERATION-ID$");
//		System.out.println("[INFO] ------------------------------------------------------------------------");
//		System.out.println("");
//		System.out.println("");
		
	}

	/**
	 * Downloads data from an object in a bucket to a local file.
	 *
	 * @param objectName
	 *            the name of the source object.
	 * @param localFilename
	 *            the name of the destination file.
	 * @param bucketName
	 *            the name of the bucket to create the object in.
	 * @param username
	 *            the cloud username.	 
	 *            
	 */
	
	public static void downloadFile(String objectName, String localFilename, String bucketName, String username)
			throws IOException, GeneralSecurityException {
		
		// Do the get
		Storage client = StorageFactory.getService();
		
		Storage.Objects.Get getObject = client.objects().get(bucketName, objectName);
		  ByteArrayOutputStream out = new ByteArrayOutputStream();
		  // If you're not in AppEngine, download the whole thing in one request, if possible.
		  getObject.getMediaHttpDownloader().setDirectDownloadEnabled(!IS_APP_ENGINE);
		  getObject.executeMediaAndDownloadTo(out);
		  
			try(OutputStream outputStream = new FileOutputStream(localFilename)) {
				out.writeTo(outputStream);
			}
	}
	
	/**
	 * Uploads data to an object in a bucket.
	 *
	 * @param name
	 *            the name of the destination object.
	 * @param contentType
	 *            the MIME type of the data.
	 * @param file
	 *            the file to upload.
	 * @param bucketName
	 *            the name of the bucket to create the object in.
	 * @param userAccount
	 *            the cloud userAccount.	 
	 */
	public static void uploadFile(String name, String contentType, File file, String bucketName, String userAccount)
			throws IOException, GeneralSecurityException {
		InputStreamContent contentStream = new InputStreamContent(contentType, new FileInputStream(file));
		// Setting the length improves upload performance
		contentStream.setLength(file.length());
		StorageObject objectMetadata = new StorageObject()
				// Set the destination object name
				.setName(name)
				// Set the access control list to publicly read-only
				.setAcl(Arrays.asList(new ObjectAccessControl().setEntity("user-" + userAccount).setRole("OWNER")));
				//.setAcl(Arrays.asList(new ObjectAccessControl().setEntity("allUsers").setRole("READER")));

		// Do the insert
		Storage client = StorageFactory.getService();
		Storage.Objects.Insert insertRequest = client.objects().insert(bucketName, objectMetadata, contentStream);

		insertRequest.execute();
	}


	/**
	 * Checks input variables and call upload function.
	 * 
	 * @param username
	 *            the cloud username.	
	 * @param cloudFilename
	 *            the name of the object on the cloud.
	 * @param localFilename
	 *            the name of the source file.
	 * @param bucketName
	 *            the name of the bucket to create the object in.
	 *     
	 */
	public static void runUpload(String username, String bucketName, String localFilenameAddr, 
			String cloudFilename) {

		if (!Strings.isNullOrEmpty(bucketName) && !Strings.isNullOrEmpty(localFilenameAddr )
				&& !Strings.isNullOrEmpty(cloudFilename))  {
			try {
				Path p = Paths.get(localFilenameAddr);
				File tempFile = p.toFile();
				uploadFile(cloudFilename, "text/plain", tempFile, bucketName, username);

			} catch (IOException e) {
				System.err.println(e.getMessage());
				System.exit(1);
			} catch (Throwable t) {
				t.printStackTrace();
				System.exit(1);
			}
			
			System.out.println("");
			System.out.println("");
			System.out.println("[INFO] ------------------------------------------------------------------------");
			System.out.println("[INFO] Successfully Uploaded " + localFilenameAddr);
			System.out.println("[INFO] Address: " + "gs://" + bucketName + "/" + cloudFilename);
			System.out.println("[INFO] ------------------------------------------------------------------------");
			System.out.println("");
			System.out.println("");

		} else {
			System.out.println("Usage: StorageSample <user-acccount> <bucket-name> <Local FileName> <Cloud Filename>");
			System.exit(1);
		}

	}
	
	/**
	 * Checks input variables and call download function.
	 * 
	 * @param username
	 * @param cloudFilename
	 *            the name of the source object.
	 * @param localFilename
	 *            the name of the source file.
	 * @param bucketName
	 *            the name of the bucket to create the object in.
	 *     
	 */
	
	public static void runDownload (String username, String bucketName, String localFilename, 
			String cloudFilename) {
		
		if (!Strings.isNullOrEmpty(bucketName) && !Strings.isNullOrEmpty(localFilename )
				&& !Strings.isNullOrEmpty(cloudFilename)) {
			try {
				downloadFile(cloudFilename, localFilename, bucketName,  username);

			} catch (IOException e) {
				System.err.println(e.getMessage());
				System.exit(1);
			} catch (Throwable t) {
				t.printStackTrace();
				System.exit(1);
			}

		} else {
			System.out.println("Usage: StorageSample <user-acccount> <bucket-name> <Local FileName> <Cloud Filename>");
			System.exit(1);
		}
	}
}
