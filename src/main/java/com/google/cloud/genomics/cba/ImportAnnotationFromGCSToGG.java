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
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.Genomics.Annotationsets;
import com.google.api.services.genomics.model.Annotation;
import com.google.api.services.genomics.model.AnnotationSet;
import com.google.api.services.genomics.model.BatchCreateAnnotationsRequest;
import com.google.api.services.genomics.model.BatchCreateAnnotationsResponse;
import com.google.api.services.genomics.model.Transcript;
import com.google.api.services.genomics.model.VariantAnnotation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy.Context;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.RetryPolicy;
import com.google.common.collect.Lists;

/**
 * <h1>Import Annotation Files (Reference Annotation Datasets)</h1> This class
 * creates an annotationSet, and imports annotations. It prepares and converts
 * Annotation fields to the corresponding fields in the Google Genomics
 * annotation set APIs.
 * 
 * @version 1.0
 * @since 2016-07-01
 */

public class ImportAnnotationFromGCSToGG {

	/*
	 * Options required to run this pipeline.
	 * 
	 * @param datasetId The ID of the Google Genomics Dataset that the output
	 * AnnotationSet will be posted.
	 * 
	 * @param annotationSetName This provides the name for the AnnotationSet.
	 * 
	 * @param annotationReferenceSetId This provides the refernceSetId for the
	 * AnnotationSet.
	 * 
	 * @param annotationOutputJSONBucketAddr This provides the URI of output
	 * Json file [This input is optional].
	 * 
	 * @param annotationInputTextBucketAddr This provides the URI of inputfile
	 * which contains annotations.
	 * 
	 * @param type This provides the type of reference annotation dataset
	 * 
	 * @param base0 This provides whether the reference annotations are 0-Based
	 * or 1-Based.
	 */
	public static interface Options extends ShardOptions {

		@Description("The ID of the Google Genomics Dataset that the output AnnotationSet will be posted to.")
		@Default.String("")
		String getDatasetId();

		void setDatasetId(String datasetId);

		@Description("This provides the name for the AnnotationSet. Default (empty) will set the "
				+ "name to the input References. For more information on AnnotationSets, please visit: "
				+ "https://cloud.google.com/genomics/v1beta2/reference/annotationSets#resource")
		@Default.String("")
		String getAnnotationSetName();

		void setAnnotationSetName(String name);

		@Description("This provides the refernceSetId for the AnnotationSet. This is a required field.")
		@Default.String("")
		String getAnnotationReferenceSetId();

		void setAnnotationReferenceSetId(String referenceSetId);

		@Description("This provides the URI of output Json file. This file contains Json version of annotations. This is a required field.")
		@Default.String("")
		String getAnnotationOutputJSONBucketAddr();

		void setAnnotationOutputJSONBucketAddr(String annotationOutputJSONBucketAddr);

		@Description("This provides the URI of inputfile which contains annotations. This is a required field.")
		@Default.String("")
		String getAnnotationInputTextBucketAddr();

		void setAnnotationInputTextBucketAddr(String annotationInputTextBucketAddr);

		@Description("This provides the type for the AnnotationSet. This is a required field.")
		@Default.String("")
		String getType();

		void setType(String type);

		@Description("This provides whether the base is 0 or 1. This is a required field.")
		@Default.String("")
		String getBase0();

		void setBase0(String base0);

		@Description("This provides the header for the Annotations. This is a required field. "
				+ "(It must start w/ the following fields: \"Chrom,start,end,ref,alter,all other fileds\")")
		@Default.String("")
		String getHeader();

		void setHeader(String header);

		@Description("This provides BigQuery Dataset ID.")
		@Default.String("")
		String getBigQueryDataset();

		void setBigQueryDataset(String BigQueryDataset);

		@Description("This provides BigQuery Table.")
		@Default.String("")
		String getBigQueryTable();

		void setBigQueryTable(String BigQueryTable);

	}

	private static Options options;
	private static Pipeline p;
	private static OfflineAuth auth;
	private static final Logger LOG = Logger.getLogger(ImportAnnotationFromGCSToGG.class.getName());

	public static void run(String[] args) throws GeneralSecurityException, IOException {
		// Register the options so that they show up via --help
		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		auth = GenomicsOptions.Methods.getGenomicsAuth(options);

		p = Pipeline.create(options);
		//p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

		if (options.getDatasetId().isEmpty()) {
			throw new IllegalArgumentException("datasetId must be specified");
		}

		if (options.getAnnotationReferenceSetId().isEmpty()) {
			throw new IllegalArgumentException("referenceSetId must be specified");
		}

		if (options.getAnnotationInputTextBucketAddr().isEmpty()) {
			throw new IllegalArgumentException("annotationInputTextBucketAddr must be specified");
		}

		// if (options.getAnnotationOutputJSONBucketAddr().isEmpty()) {
		// throw new IllegalArgumentException("AnnotationOutputJSONBucketAddr
		// must be specified");
		// }

		if (options.getType().isEmpty()) {
			throw new IllegalArgumentException("Type must be specified");
		}

		if (options.getHeader().isEmpty()) {
			throw new IllegalArgumentException("Header must be specified");
		}

		if (options.getBase0().isEmpty()) {
			throw new IllegalArgumentException("Base0 must be specified");
		}

		boolean baseStatus = false;
		if (options.getBase0().equalsIgnoreCase("YES")) {
			baseStatus = true;
		} else if (options.getBase0().equalsIgnoreCase("NO")) {
			baseStatus = false;
		} else {
			throw new IllegalArgumentException("Base0 option must be either yes or no!");
		}

		AnnotationSet annotationSet = createAnnotationSet();

		 //If user specify OutputJSONBucketAddr then the program creates the file.
		if (options.getAnnotationOutputJSONBucketAddr().isEmpty()) {
			p.apply(TextIO.read().from(options.getAnnotationInputTextBucketAddr()))
					.apply(ParDo.of(new CreateAnnotations(annotationSet.getId(), annotationSet.getType(),
							options.getHeader(), baseStatus, auth, true)));
		} else {
			p.apply(TextIO.read().from(options.getAnnotationInputTextBucketAddr()))
					.apply(ParDo.of(new CreateAnnotations(annotationSet.getId(), annotationSet.getType(),
							options.getHeader(), baseStatus, auth, true)))
					.apply(TextIO.write().to(options.getAnnotationOutputJSONBucketAddr()));
		}
		
		p.run().waitUntilFinish();


		System.out.println("");
		System.out.println("");
		System.out.println("[INFO] ------------------------------------------------------------------------");
		System.out.println("[INFO] To check the current status of your job, use the following command:");
		System.out.println("\t ~: gcloud alpha dataflow jobs describe $YOUR_JOB_ID$");
		System.out.println("[INFO] ------------------------------------------------------------------------");
		System.out.println("");
		System.out.println("");

	}


	/*
	 * TODO: If a particular batchCreateAnnotations request fails more than 4
	 * times and the bundle gets retried by the dataflow pipeline, some
	 * annotations may be written multiple times.
	 */
	static class CreateAnnotations extends
			// DoFn<KV<Position, Iterable<KV<String, String>>>, Annotation> {
			DoFn<String, String> {
		private static final long serialVersionUID = 1L;
		private final String asId;
		private final String asType;
		private final String header;
		private final boolean is_0_Base;
		private final OfflineAuth auth;
		private final List<Annotation> currAnnotations;
		private final boolean write;

		public CreateAnnotations(String asId, String asType, String header, boolean is_0_Base, OfflineAuth auth,
				boolean write) {
			this.asId = asId;
			this.asType = asType;
			this.header = header;
			this.is_0_Base = is_0_Base;
			this.auth = auth;
			this.currAnnotations = Lists.newArrayList();
			this.write = write;
		}

		private static long waitingTime = 0;

		// VariantAnnotation
		@org.apache.beam.sdk.transforms.DoFn.ProcessElement
		public void processElement(ProcessContext c) throws GeneralSecurityException, IOException {

			for (String line : c.element().split("\n")) {
				if (!line.isEmpty() && !line.startsWith("#") && !line.startsWith("chrom")) {
					String[] words = line.split("\t");
					String[] fields = this.header.split(",");

					Annotation a = new Annotation();
					HashMap<String, String> mapInfo = new HashMap<String, String>();

					if (asType.equalsIgnoreCase("GENERIC") && words.length >= 3) {

						a.setType("GENERIC").setAnnotationSetId(asId).setReferenceName(canonicalizeRefName(words[0]))
								.setInfo(new HashMap<String, List<Object>>());

						if (this.is_0_Base) {
							a.setStart(Long.parseLong(words[1])).setEnd(Long.parseLong(words[2]));
						} else { // 1-Base
							a.setStart(Long.parseLong(words[1]) - 1).setEnd(Long.parseLong(words[2]));
						}
						for (int index = 3; index < fields.length; index++) {
							mapInfo.put(fields[index], words[index]);
						}
					}

					else if (asType.equalsIgnoreCase("VARIANT") && words.length >= 5) {

						VariantAnnotation va = new VariantAnnotation();
						/*Make sure to handle special case null cases for alternate bases*/
						if (words[4] == null || words[4].isEmpty() || words[4] =="-")
							va.setAlternateBases("");
						else // Add alternateBases
							va.setAlternateBases(
									checkBases(words[4], Long.parseLong(words[1]), Long.parseLong(words[2])));

						a.setType("VARIANT").setAnnotationSetId(asId).setReferenceName(canonicalizeRefName(words[0]))
								.setVariant(va).setInfo(new HashMap<String, List<Object>>());
  
						if (this.is_0_Base) {
							a.setStart(Long.parseLong(words[1])).setEnd(Long.parseLong(words[2]));
						} else { // 1-Base
							a.setStart(Long.parseLong(words[1]) - 1).setEnd(Long.parseLong(words[2]));
						}

						// fields[3] or words[3] is REF, and comes from
						// variant!!!

						for (int index = 5; index < fields.length; index++) {
							mapInfo.put(fields[index], words[index]);
						}

					}
					// TODO:At this point, our system only supports Generic and
					// Variant Annotations
					else if (asType.equalsIgnoreCase("TRANSCRIPT") && words.length >= 4) {

						Transcript t = new Transcript();
						t.setGeneId(words[3]);
						a.setType("TRANSCRIPT").setAnnotationSetId(asId).setReferenceName(canonicalizeRefName(words[0]))
								.setTranscript(t).setInfo(new HashMap<String, List<Object>>());

						if (this.is_0_Base) {
							a.setStart(Long.parseLong(words[1])).setEnd(Long.parseLong(words[2]));
						} else { // 1-Base
							a.setStart(Long.parseLong(words[1]) - 1).setEnd(Long.parseLong(words[2]));
						}

						for (int index = 4; index < fields.length; index++) {
							mapInfo.put(fields[index], words[index]);
						}

					} else {
						LOG.warning("invalid input: " + line);
					}

					/* Adding Info <Keys, Values> */
					Iterator<Entry<String, String>> it = mapInfo.entrySet().iterator();
					while (it.hasNext()) {
						Entry<String, String> pair = it.next();
						List<Object> output = Lists.newArrayList();
						output.add(pair.getValue());
						a.getInfo().put(pair.getKey().toString(), output);
					}

					if (write) {
						currAnnotations.add(a);
						if (currAnnotations.size() == 512) {
							// Batch create annotations once we hit the max
							// amount for a batch
							batchCreateAnnotations();
						}
					}
					c.output(a.toString());
				}
			}

		}

		// TODO: Add more QC conditions here
		/**
		 * Check alteranteBases; e.g., 032CA -> Report Error;
		 * 
		 * @param alternateBases
		 * @param start
		 * @param end
		 * @return String
		 */
		private static String checkBases(String bases, long start, long end) {
			char[] c_arr = bases.toCharArray();
			for (char c : c_arr) {
				if (c != 'A' && c != 'T' && c != 'C' && c != 'G' && c != ' ') {
					LOG.warning("Error: Alternate bases must be T, C, G, A; Bases for (" + start + "," + end + ") is: "
							+ bases);
					throw new IllegalArgumentException("Alternate bases must be a sequence of TCGA!  "
							+ "Wrong Alternate Bases for (" + start + "," + end + ") is: " + bases);
				}

			}

			return bases;
		}

		/**
		 * <h1>This function remove "chr" from the beginning of referenceName
		 * (e.g, chr17 becomes 17)
		 * 
		 * @param refName
		 *            Reference Name
		 * @return refName Reference Name w/o "chr"
		 */
		private static String canonicalizeRefName(String refName) {
			return refName.replace("chr", "");
		}

		/**
		 * <h1>If the number of annotations in the last batch is less than the
		 * specified size of batch (e.g., 4096) then this method post the last
		 * batch.
		 */
		public void finishBundle(Context c) throws IOException, GeneralSecurityException {
			// Finish up any leftover annotations at the end.
			if (write && !currAnnotations.isEmpty()) {
				batchCreateAnnotations();
			}
			LOG.warning("Waiting Time: " + waitingTime);
		}

		/**
		 * <h1>This method creates and post a batch of annotations to Google
		 * Genomics. It also uses exponential backoff.
		 * 
		 */
		private void batchCreateAnnotations() throws IOException, GeneralSecurityException {

			ExponentialBackOff backoff = new ExponentialBackOff.Builder().build();
			/**
			 * The default maximum elapsed time in milliseconds (15 minutes).
			 */
			/** The default maximum back off time in milliseconds (1 minute). */

			while (true) {
				try {
					Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);
					Genomics.Annotations.BatchCreate aRequest = genomics.annotations()
							.batchCreate(new BatchCreateAnnotationsRequest().setAnnotations(currAnnotations));
					RetryPolicy retryP = RetryPolicy.nAttempts(4);
					BatchCreateAnnotationsResponse responseA = retryP.execute(aRequest);
					/*
					 * TODO: To keep track of batch requests, and remove the
					 * possibility of duplications, Google Genomics added
					 * "requestId": string". The request id is not yet in the
					 * Java client. It will be in the next release of
					 * utils-java. Genomics.Annotations.BatchCreate aRequest =
					 * genomics.annotations() .batchCreate(new
					 * BatchCreateAnnotationsRequest()
					 * .setAnnotations(currAnnotations),
					 * Integer.toString(Objects.hash(currAnnotations)));
					 */

					LOG.warning(responseA.toPrettyString());
					currAnnotations.clear();
					return;
				} catch (Exception e) {
					if (e instanceof java.net.SocketTimeoutException
							|| e instanceof com.google.api.client.googleapis.json.GoogleJsonResponseException) {
						// if (e.getMessage().startsWith("exception thrown while
						// executing request")) {
						LOG.warning("Backing-off per: " + e.toString());
						long backOffMillis = backoff.nextBackOffMillis();
						if (backOffMillis == BackOff.STOP) {
							throw e;
						}
						try {
							Thread.sleep(backOffMillis);
							waitingTime += backOffMillis;
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							LOG.warning("Annotations: " + currAnnotations.toString());
							e1.printStackTrace();
						}
					} else {
						throw e;
					}
				}
			}
		}
	}

	/**
	 * <h1>This method creates a new variant set using the given datasetId.
	 */
	private static AnnotationSet createAnnotationSet() throws GeneralSecurityException, IOException {

		AnnotationSet as = new AnnotationSet();
		as.setDatasetId(options.getDatasetId());

		if (!"".equals(options.getAnnotationSetName())) {
			as.setName(options.getAnnotationSetName());
		}

		as.setReferenceSetId(options.getAnnotationReferenceSetId());

		if (options.getType().equalsIgnoreCase("VARIANT"))
			as.setType("VARIANT");
		else if (options.getType().equalsIgnoreCase("TRANSCRIPT"))
			as.setType("TRANSCRIPT");
		else if (options.getType().equalsIgnoreCase("GENERIC"))
			as.setType("GENERIC");
		else {
			LOG.warning("Annotation Type is Wrong!");
			System.exit(1);
		}

		// Add Header to the Info fields
		as.setInfo(new HashMap<String, List<Object>>());
		HashMap<String, String> mapInfo = new HashMap<String, String>();
		mapInfo.put("header", options.getHeader());

		/* Adding Info <Keys, Values> */
		Iterator<Entry<String, String>> it = mapInfo.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> pair = it.next();
			List<Object> output = Lists.newArrayList();
			output.add(pair.getValue());
			as.getInfo().put(pair.getKey().toString(), output);
		}

		Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);
		Annotationsets.Create asRequest = genomics.annotationsets().create(as);
		AnnotationSet asWithId = asRequest.execute();
		System.out.println(asWithId.toPrettyString());
		return asWithId;
	}

}
