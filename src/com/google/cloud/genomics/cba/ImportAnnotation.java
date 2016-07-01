package com.google.cloud.genomics.cba;

/*
 * Copyright (C) 2015 Google Inc.
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
import java.util.Map;

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.Genomics.Annotationsets;
import com.google.api.services.genomics.model.Annotation;
import com.google.api.services.genomics.model.AnnotationSet;
import com.google.api.services.genomics.model.BatchCreateAnnotationsRequest;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.RetryPolicy;
import com.google.common.collect.Lists;


/**
 * <h1>Import Annotation Files (Reference Annotation Datasets)</h1> This class creates an annotationSet, and imports annotations.
 * It prepares and converts Annotation fields to the corresponding fields in the
 * Google Genomics annotation set APIs.
 * 
 * @param outputDatasetId
 *            The ID of the Google Genomics Dataset that the output AnnotationSet will be posted.
 * @param annotationSetName
 *            This provides the name for the AnnotationSet.
 * @param annotationReferenceSetId
 *            This provides the refernceSetId for the AnnotationSet.
 * @param annotationOutputJSONBucketAddr          
 * 			  This provides the URI of output Json file.
 * @param annotationInputTextBucketAddr          
 * 			  This provides the URI of inputfile which contains annotations.
 * @version 1.0
 * @since 2016-07-01
 */

public class ImportAnnotation {

	/**
	 * Options required to run this pipeline.
	 *
	 */
	public static interface Options extends ShardOptions {


		@Validation.Required
		@Description("The ID of the Google Genomics Dataset that the output AnnotationSet will be posted to.")
		@Default.String("")
		String getOutputDatasetId();
		void setOutputDatasetId(String outputDatasetId);

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
	
		
	}

	private static Options options;
	private static Pipeline p;
	private static OfflineAuth auth;

	public static void run(String[] args) throws GeneralSecurityException, IOException {
		// Register the options so that they show up via --help
		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		auth = GenomicsOptions.Methods.getGenomicsAuth(options);

		p = Pipeline.create(options);
		p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

		if (options.getOutputDatasetId().isEmpty()) {
			throw new IllegalArgumentException("InputDatasetId must be specified");
		}

		if (options.getAnnotationReferenceSetId().isEmpty()) {
			throw new IllegalArgumentException("ReferenceSetId must be specified");
		}
		
		if (options.getAnnotationInputTextBucketAddr().isEmpty()) {
			throw new IllegalArgumentException("AnnotationInputTextBucketAddr must be specified");
		}

		if (options.getAnnotationOutputJSONBucketAddr().isEmpty()) {
			throw new IllegalArgumentException("AnnotationOutputJSONBucketAddr must be specified");
		}
		
		//options.setSta
		
		AnnotationSet annotationSet = createAnnotationSet();
		

		p.apply(TextIO.Read.from(options.getAnnotationInputTextBucketAddr()))
				.apply(ParDo.of(new CreateAnnotations(annotationSet.getId(), auth, true)))
				.apply(TextIO.Write.to(options.getAnnotationOutputJSONBucketAddr()));

		p.run();

	}

	/*
	 * TODO: If a particular batchCreateAnnotations request fails more than 4
	 * times and the bundle gets retried by the dataflow pipeline, some
	 * annotations may be written multiple times.
	 */
	static class CreateAnnotations extends
			// DoFn<KV<Position, Iterable<KV<String, String>>>, Annotation> {
			DoFn<String, String> {

		private final String asId;
		private final OfflineAuth auth;
		private final List<Annotation> currAnnotations;
		private final boolean write;

		public CreateAnnotations(String asId, OfflineAuth auth, boolean write) {
			this.asId = asId;
			this.auth = auth;
			this.currAnnotations = Lists.newArrayList();
			this.write = write;
		}

		//TODO: Find automatically Start, End, ReferenceName, and add everything else into the info field!    
		@Override
		public void processElement(ProcessContext c) throws GeneralSecurityException, IOException {
			for (String line : c.element().split("\n")) {
				if (!line.isEmpty()) {
					String[] words = line.split("\t");
					long position = Long.parseLong(words[1]);
					HashMap<String, String> mapInfo = new HashMap <String, String>();
					
					mapInfo.put("ref", words[2]); 
					mapInfo.put("alternateBases", words[3]); 
					mapInfo.put("Allele_frequency", words[4]);  
					mapInfo.put("dbSNPId", words[5]);
					
					
					Annotation a = new Annotation()
							.setAnnotationSetId(asId)
							.setStart(position)
							.setEnd(position)
							.setReferenceName(words[0])
							.setType("GENERIC")
							.setInfo(new HashMap<String, List<Object>>());
					
					
					Iterator it = mapInfo.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry pair = (Map.Entry)it.next();
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
		
		/**
		 * If the number of annotations in the last batch is <500 then this method post the last batch.
		 */

		@Override
		public void finishBundle(Context c) throws IOException, GeneralSecurityException {
			// Finish up any leftover annotations at the end.
			if (write && !currAnnotations.isEmpty()) {
				batchCreateAnnotations();
			}
		}
		
		/**
		 * This method create and post a batch of annotations to Google genomics.
		 */
		private void batchCreateAnnotations() throws IOException, GeneralSecurityException {
			Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);
			Genomics.Annotations.BatchCreate aRequest = genomics.annotations()
					.batchCreate(new BatchCreateAnnotationsRequest().setAnnotations(currAnnotations));
			RetryPolicy retryP = RetryPolicy.nAttempts(4);
			retryP.execute(aRequest);
			currAnnotations.clear();
		}
	}

	
	/**
	 * This method creates a new variant set using the given datasetId.
	 */
	
	private static AnnotationSet createAnnotationSet() throws GeneralSecurityException, IOException {

		AnnotationSet as = new AnnotationSet();
		as.setDatasetId(options.getOutputDatasetId());

		if (!"".equals(options.getAnnotationSetName())) {
			as.setName(options.getAnnotationSetName());
		}

		as.setReferenceSetId(options.getAnnotationReferenceSetId());
		as.setType("GENERIC");
		Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);
		Annotationsets.Create asRequest = genomics.annotationsets().create(as);
		AnnotationSet asWithId = asRequest.execute();
		System.out.println(asWithId.toPrettyString());
		return asWithId;
	}
}
