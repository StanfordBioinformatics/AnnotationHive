package com.google.cloud.genomics.cba;


/**
 * Uploading and Downloading files - Cloud Storage
 * Modified version - Google Class - Handles uploads and downloads 
 * 
 */

/*
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.ObjectAccessControl;
import com.google.api.services.storage.model.StorageObject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Arrays;

public class CloudStorage {

	private static final boolean IS_APP_ENGINE = false;


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
		boolean getMetadata = false;
		
		Storage.Objects.Get getObject = client.objects().get(bucketName, objectName);

		if (getMetadata == true) {
		  StorageObject object = getObject.execute();
		} else {
		  // Downloading data.
		  ByteArrayOutputStream out = new ByteArrayOutputStream();
		  // If you're not in AppEngine, download the whole thing in one request, if possible.
		  getObject.getMediaHttpDownloader().setDirectDownloadEnabled(!IS_APP_ENGINE);
		  getObject.executeMediaAndDownloadTo(out);
		  
			try(OutputStream outputStream = new FileOutputStream(localFilename)) {
				out.writeTo(outputStream);
			}
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
	 * @param username
	 *            the cloud username.	 
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
	 * @param objectName
	 *            the name of the source object.
	 * @param localFilename
	 *            the name of the source file.
	 * @param bucketName
	 *            the name of the bucket to create the object in.
	 *     
	 */
	public static void runUpload(String username, String bucketName, String localFileName, 
			String cloudFileName) {

		if ((bucketName != null && !bucketName.isEmpty()) && (localFileName != null && !localFileName.isEmpty())
				&& (cloudFileName != null && !cloudFileName.isEmpty())) {
			try {
				// Create a temp file to upload
				Path tempPath = Files.createTempFile(localFileName, "txt");
				Files.write(tempPath, "Sample file".getBytes());
				File tempFile = tempPath.toFile();
				tempFile.deleteOnExit();
				// Upload it
				uploadFile(cloudFileName, "text/plain", tempFile, bucketName, username);

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
	
	/**
	 * Checks input variables and call download function.
	 * 
	 * @param username
	 *            the cloud username.	
	 * @param objectName
	 *            the name of the source object.
	 * @param localFilename
	 *            the name of the source file.
	 * @param bucketName
	 *            the name of the bucket to create the object in.
	 *     
	 */
	
	public static void runDownload (String username, String bucketName, String localFileName, 
			String cloudFileName) {

		if ((bucketName != null && !bucketName.isEmpty()) && (localFileName != null && !localFileName.isEmpty())
				&& (cloudFileName != null && !cloudFileName.isEmpty())) {
			try {
				downloadFile(cloudFileName, localFileName, bucketName,  username);

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
