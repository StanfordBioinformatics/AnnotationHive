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
import com.google.api.client.util.Strings;
import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.Annotation;
import com.google.api.services.genomics.model.AnnotationSet;
import com.google.api.services.genomics.model.SearchAnnotationsRequest;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import com.google.cloud.genomics.dataflow.utils.CallSetNamesOptions;
import com.google.cloud.genomics.dataflow.utils.GCSOutputOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.dataflow.utils.ShardOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.OfflineAuth;
import com.google.cloud.genomics.utils.Paginator;
import com.google.cloud.genomics.utils.ShardBoundary;
import com.google.cloud.genomics.utils.ShardUtils;
import com.google.cloud.genomics.utils.grpc.VariantStreamIterator;
import com.google.cloud.genomics.utils.grpc.VariantUtils;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Range;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;
import com.google.genomics.v1.VariantCall;

import htsjdk.samtools.util.IntervalTree;
import htsjdk.samtools.util.IntervalTree.Node;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * <h1>Annotating Google Genomics Variant Sets</h1> This class an annotationSet,
 * and imports annotations. It prepares and converts Annotation fields to the
 * corresponding fields in the Google Genomics annotation set APIs.
 * 
 * @param variantSetId
 *            The ID of the Google Genomics variant set this pipeline is
 *            accessing.
 * @param callSetNames
 *            This provides the name for the AnnotationSet.
 * @param transcriptSetIds
 *            The IDs of the Google Genomics transcript sets.
 * @param variantAnnotationSetIds
 *            The IDs of the Google Genomics variant annotation sets.
 * @param supportChrM
 *         Some annotation reference datasets do not provide information about ChrM; therefore, before
 *         searching for chrM, user must set this variable TRUE. NOTE: All annotation sets in the query must support chrM. 
 *         Otherwise, the query will be failed due to "error 404: Not Found". Default value is FALSE.
 * @param onlySNP
 * 			By setting this value to TRUE, the software only annotate SNPs. Default value is FALSE.            
 *            
 * @version 1.0
 * @since 2016-07-01
 */

final class AnnotateVariants extends DoFn<StreamVariantsRequest, KV<String, String>> {

	public static boolean DEBUG = false;
	private static HashMap<String, Integer> ColInfoTranscript = new HashMap<String, Integer>();
	private static HashMap<String, Integer> ColInfoVariantAnnotation = new HashMap<String, Integer>();
	private static String HEADER="#referenceName	start	end	referenceBases	alternateBases	quality";

	
	
	public static interface Options extends
			// Options for call set names.
			CallSetNamesOptions,
			// Options for calculating over regions, chromosomes, or whole
			// genomes.
			ShardOptions,
			// Options for the output destination.
			GCSOutputOptions {

		@Override
		@Description("The ID of the Google Genomics variant set this pipeline is accessing.")
		@Default.String("")
		String getVariantSetId();

		@Override
		@Description("The names of the Google Genomics call sets this pipeline is working with, comma " + "delimited.")
		@Default.String("")
		String getCallSetNames();

		@Description("The IDs of the Google Genomics transcript sets this pipeline is working with, "
				+ "comma delimited.")
		@Default.String("")
		String getTranscriptSetIds();

		void setTranscriptSetIds(String transcriptSetIds);

		@Description("The IDs of the Google Genomics variant annotation sets this pipeline is working "
				+ "with, comma delimited.")
		@Default.String("")
		String getVariantAnnotationSetIds();
		void setVariantAnnotationSetIds(String variantAnnotationSetIds);

		@Description("This provides whether all the annotation sets support Chromosome M or not. Default is false. ")
		boolean getSupportChrM();
		void setSupportChrM(boolean supportChrM);
		
		@Description("If you want to only annotate SNPs, set this value true. Default is false. ")
		boolean getOnlySNP();
		void setOnlySNP(boolean onlySNP);
		
		public static class Methods {
			public static void validateOptions(Options options) {
				GCSOutputOptions.Methods.validateOptions(options);
			}
		}
	}

	private static final Logger LOG = Logger.getLogger(AnnotateVariants.class.getName());

	/*
	 * To visit variants efficiently, the program creates "VariantStreamIterator" by considering "VARIANT_FIELDS". 
	 * VARIANT_FIELDS represents the structure of response. By specifying VARIANT_FIELDS, we can avoid loading extra information.
	 * Tip: Use the API explorer to test which fields to include in partial responses.
	 * https://developers.google.com/apis-explorer/#p/genomics/v1/genomics.variants.stream 
	*/ 
	private static final String VARIANT_FIELDS = "variants(id,referenceName,start,end,alternateBases,referenceBases,calls,quality)";

	private final OfflineAuth auth;
	private final List<String> callSetIds, transcriptSetIds, variantAnnotationSetIds;
	private final HashMap<String, Integer> VariantColInfo;
	private final HashMap<String, Integer> TranscriptColInfo;
	private final boolean supportChrM;
	private final boolean onlySNP;

	public AnnotateVariants(OfflineAuth auth, List<String> callSetIds, List<String> transcriptSetIds,
			List<String> variantAnnotationSetIds, HashMap<String, Integer> _VariantColInfo, 
			HashMap<String, Integer> _TranscriptColInfo, boolean _supportChrM, boolean _onlySNP) {
		this.auth = auth;
		this.callSetIds = callSetIds;
		this.transcriptSetIds = transcriptSetIds;
		this.variantAnnotationSetIds = variantAnnotationSetIds;
		this.VariantColInfo = _VariantColInfo;
		this.TranscriptColInfo = _TranscriptColInfo;
		this.supportChrM = _supportChrM;
		this.onlySNP=_onlySNP;
	}

	@Override
	public void processElement(DoFn<StreamVariantsRequest, KV<String, String>>.ProcessContext c) throws Exception {
		
		Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);

		StreamVariantsRequest request = StreamVariantsRequest.newBuilder(c.element()).addAllCallSetIds(callSetIds)
				.build();
		
		//TODO: It would be better to exclude chrM from the variantStream  
		if (canonicalizeRefName(request.getReferenceName()).equals("M") && supportChrM == false) {
			LOG.info("There is no information about Chr M in the provided AnnotationSet!");
			return;
		}
		
		Iterator<StreamVariantsResponse> iter = VariantStreamIterator.enforceShardBoundary(auth, request,
				ShardBoundary.Requirement.STRICT, VARIANT_FIELDS);

		if (!iter.hasNext()) {
			LOG.info("region has no variants, skipping");
			return;
		}

		Stopwatch stopwatch = Stopwatch.createStarted();
		int varCount = 0;

		ListMultimap<Range<Long>, Annotation> variantAnnotations = null;
		if (this.variantAnnotationSetIds != null)
			variantAnnotations = retrieveVariantAnnotations(genomics, request);

		IntervalTree<Annotation> transcripts = null;
		if (this.transcriptSetIds != null)
			transcripts = retrieveTranscripts(genomics, request);

		while (iter.hasNext()) {
			Iterable<Variant> varIter;
			if(onlySNP)
				varIter = FluentIterable.from(iter.next().getVariantsList()).filter(VariantUtils.IS_SNP);
			else
			    varIter = FluentIterable.from(iter.next().getVariantsList());
	
			for (Variant variant : varIter) {
				Range<Long> pos = Range.closedOpen(variant.getStart(), variant.getEnd());

				// This variable helps to keep track of alignment
				String VCFOutput = "";

				// Variant Annotation Section
				if (variantAnnotations != null) {
		
					// Sort the list of matched annotations
					SortedSet<String> VariantAnnotationKeys = new TreeSet<String>(VariantColInfo.keySet());
		
					// Retrieve a list of matched variant annotations
					List<Annotation> listMatchedAnnotations = variantAnnotations.get(pos);

					// Visit overlapped annotations in order, and the matches in order (First convert to VCF format, and then add it to VCFOutput);
					int index=0;
					for (String key : VariantAnnotationKeys) {
						//The following variables help to put a semicolon between multiple matches from the same annotationSet 
						// e.g., allele_freq1;allele_freq2;...;allele_freqn;
						boolean SemiColon=false;
						for (Annotation match : listMatchedAnnotations) {
							if (match.getAnnotationSetId().compareTo(key) == 0) {
								if ((match.getVariant().getAlternateBases() != null
										&& variant.getAlternateBasesList() != null)) {
									// check if Variant's alternate bases are
									// the same as the matched annotation's
									// alternate bases
									if (compareAlternateBases(match.getVariant().getAlternateBases(),
											variant.getAlternateBasesList())) {
	
										if (DEBUG)
											LOG.info("MATCHED: variant: (" + variant.getStart()  + ", Annotation: " + match.getStart() + ") ");
										
										if(!SemiColon){
											VCFOutput += createVCFFormat(variant, match);
											SemiColon=true;
											//Activate it for the next matched element
										}
										else{
											VCFOutput += ";" + createVCFFormat(variant, match);
										}
									}
								}
							}
						}
						index++;
						/* formatTabs function helps to keep track of alignment in the
						 * VCF format (e.g., if there is no match for Variant X in AnnotationSet Y then add spaces
						 * equals to the number of AnnotationSet Y's columns in the VCF file)
						 */
						if (VCFOutput.isEmpty() && (VariantAnnotationKeys.size() > index || TranscriptColInfo.size() > 0)) {
							VCFOutput += formatTabs(VariantColInfo.get(key));
						}
					}// end of keys
				} //End of Variant Annotation

				// Transcript Annotation Section
				if (transcripts != null) {

					// Find all the overlapped matches and create an interval tree
					Iterator<Node<Annotation>> transcriptIter = transcripts.overlappers(pos.lowerEndpoint().intValue(),
							pos.upperEndpoint().intValue() - 1); // Inclusive.

					Iterator<Node<Annotation>> StartPoint = transcriptIter;
					if (transcriptIter != null) {
						// Sort the list of matched annotations
						SortedSet<String> transcriptKeys = new TreeSet<String>(TranscriptColInfo.keySet());
						int index=0;
						// Check annotations in order, and in the case of match convert the matches to VCF format
						for (String key : transcriptKeys) {
							transcriptIter = StartPoint;
							boolean SemiColon=false;
							while (transcriptIter.hasNext()) {
								Annotation transcript = transcriptIter.next().getValue();
								if (transcript.getAnnotationSetId().compareTo(key) == 0) {
									if(!SemiColon){
										VCFOutput += createVCFFormat(variant, transcript);
										SemiColon=true;
									}
									else
										VCFOutput += ";" + createVCFFormat(variant, transcript);
								}
							}
							index++;

							if (VCFOutput.isEmpty() && transcriptKeys.size()>index) {
								VCFOutput += formatTabs(TranscriptColInfo.get(key));
							}
						}

					}
				} // End of Transcripts

				
				String ALTs = "";
				for (int index = 0; index < variant.getAlternateBasesCount(); index++)
					ALTs += variant.getAlternateBases(index);

				
				//The following section helps to add genotypes 
				String VariantGenotype="";
				List<VariantCall> Genotypes = variant.getCallsList(); 
				
				for(String CId: callSetIds){
					for(VariantCall VC:Genotypes){
						if(VC.getCallSetId().equals(CId)){
							
							List<Integer> GentotypeList = VC.getGenotypeList();
							for(int index=0; index < GentotypeList.size(); index++){
								int Genotype = GentotypeList.get(index);
		
								if(index>0)
									VariantGenotype += "/";
								
								VariantGenotype += Genotype;
							}
						}
					}
					VariantGenotype += ";";
				}
				
				
				/*Emit the information in the form of <Key, Value> 
				 * Print out the variant w/ or w/o any matched annotations
				 * Key: (ChromId, Start, End) 
				 * Value:(variant's <referenceName	start	end	referenceBases	alternateBases	quality>, 
				 * + The content of "VCFOutput" OR Annotation's fields
				 */
				c.output(KV.of(
						variant.getReferenceName() + ";" + Long.toString(variant.getStart()) + ";"
								+ Long.toString(variant.getEnd()),
								//Value
								variant.getReferenceName() 
								// <-- increment by 1 => convert to 1-based -->
								+ "\t" + (variant.getStart() + 1) 
								+ "\t" + variant.getEnd() 
								+ "\t" + variant.getReferenceBases() 
								+ "\t" + ALTs 
								+ "\t" + variant.getQuality()
								+ "\t" + VariantGenotype 
								+ "\t" + VCFOutput));
				
				
				varCount++;
				if (varCount % 1e3 == 0) {
					LOG.info(String.format("read %d variants (%.2f / s)", varCount,
							(double) varCount / stopwatch.elapsed(TimeUnit.SECONDS)));
				}
			}

		}

		LOG.info("finished reading " + varCount + " variants in " + stopwatch);
	}

	/**
	 * <h1>This function compares Variant's alternateBases with the potential matched annotation.
	 * @param	alternateBases  
	 *				The potential matched annotation's alternateBases
	 * @param 	variantAlternateBasesList
	 * 				The variant's alternate bases   
	 * @return     
	 */
	private boolean compareAlternateBases(String alternateBases, List<String> variantAlternateBasesList) {

		
		if (alternateBases.isEmpty() && variantAlternateBasesList.isEmpty())
		{
			LOG.info("Empty Matched!");
			return true;
		}
		
		String vString = "";
		for (String s : variantAlternateBasesList)
			vString += s;
		
		if (alternateBases.equals(vString))
		{
			LOG.info("Matched: Variant Alt: " + vString + ", Annotation Alt:" + alternateBases);

			return true;
		}

		LOG.info("Not Matched: Variant Alt: " + vString + ", Annotation Alt:" + alternateBases);

		return false;
	}

	
	
	/**
	 * <h1>This function prepares the matched annotation's information in the form of VCF (extended column).
	 * @param	variant  
	 * @param 	match
	 * 				The matched annotation   
	 * @return  AnnotationMap
	 * 			VCFFormat of annotation	   
	 */
	private String createVCFFormat(Variant variant, Annotation match) {
		/*
		 * VCF format: #CHROM POS ID REF ALT QUAL FILTER INFO FORMAT Sample1
		 * Sample2 Sample3
		 */

		String AnnotationMap = "";

		/* Sorting Annotation Info items based on key */
		SortedSet<String> keys = new TreeSet<String>(match.getInfo().keySet());
		for (String key : keys) {
			String stringValues = "";
			List<Object> objectValue = match.getInfo().get(key);
			for (Object value : objectValue)
				stringValues += value.toString();
			// AnnotationMap += key + "=" + stringValues + ";"; //INFO Field in
			// VCF
			if(DEBUG)
				AnnotationMap += key + "=" + stringValues + "\t"; // Extended Column
			else
				AnnotationMap += stringValues + "\t"; // Extended Column
		}

		if (DEBUG) {
			if (match.getType().equalsIgnoreCase("VARIANT"))
				AnnotationMap += "\t Variant (Ref: " + variant.getReferenceBases() + ", Alt: "
						+ variant.getAlternateBasesList().toString() + ") " + "Annotation ("
						+ match.getVariant().getAlternateBases() + ")";

			AnnotationMap += "\t Annotation[S:" + match.getStart() + ", E:" + match.getEnd() + ")";
		}

		return AnnotationMap;
	}
	
	
	/**
	 * <h1>This function first send a request to Google Genomics to receive all Generic/Transcript annotations (transcriptSetIds), 
	 * 		and  after receiving the information, it creates and return back an interval tree of all the overlapped annotations.
	 * @param	genomics 
	 * 				Genomics 
	 * @param 	request
	 * 				StreamVariantsRequest  
	 * @return  transcripts
	 * 				IntervalTree<Annotation>	   
	 */

	private IntervalTree<Annotation> retrieveTranscripts(Genomics genomics, StreamVariantsRequest request) {
		Stopwatch stopwatch = Stopwatch.createStarted();
		IntervalTree<Annotation> transcripts = new IntervalTree<>();
			Iterable<Annotation> transcriptIter = Paginator.Annotations.create(genomics, ShardBoundary.Requirement.OVERLAPS)
					.search(new SearchAnnotationsRequest().setAnnotationSetIds(transcriptSetIds)
							.setReferenceName(canonicalizeRefName(request.getReferenceName())).setStart(request.getStart())
							.setEnd(request.getEnd()));
			for (Annotation annotation : transcriptIter) {
				transcripts.put(annotation.getStart().intValue(), annotation.getEnd().intValue(), annotation);
			}
			LOG.info(String.format("read %d transcripts in %s (%.2f / s)", transcripts.size(), stopwatch,
					(double) transcripts.size() / stopwatch.elapsed(TimeUnit.SECONDS)));
		return transcripts;
	}

	/**
	 * <h1>This function first send a request to Google Genomics to receive all variant annotations (variantAnnotationSetIds), 
	 * 		and after receiving the information, it creates and return back a ListMultimap that 
	 * 		contains all the overlapped annotations plus their positions.
	 * @param	genomics 
	 * 				Genomics 
	 * @param 	request
	 * 				StreamVariantsRequest  
	 * @return  annotationMap
	 * 				ListMultimap<Range<Long>, Annotation>	   
	 */
	
	private ListMultimap<Range<Long>, Annotation> retrieveVariantAnnotations(Genomics genomics,
			StreamVariantsRequest request) {
		Stopwatch stopwatch = Stopwatch.createStarted();
		ListMultimap<Range<Long>, Annotation> annotationMap = ArrayListMultimap.create();
			Iterable<Annotation> annotationIter = Paginator.Annotations
					.create(genomics, ShardBoundary.Requirement.OVERLAPS)
					.search(new SearchAnnotationsRequest().setAnnotationSetIds(variantAnnotationSetIds)
							.setReferenceName(canonicalizeRefName(request.getReferenceName()))
							.setStart(request.getStart()).setEnd(request.getEnd()));

			for (Annotation annotation : annotationIter) {
				long start = 0;
				if (annotation.getStart() != null) {
					start = annotation.getStart();
				}
				annotationMap.put(Range.closedOpen(start, annotation.getEnd()), annotation);
			}

			LOG.info(String.format("read %d variant annotations in %s (%.2f / s)", annotationMap.size(), stopwatch,
					(double) annotationMap.size() / stopwatch.elapsed(TimeUnit.SECONDS)));

		return annotationMap;
	}

	/**
	 * <h1>This function remove "chr" from the beginning of referenceName (e.g, --references=chr17:1:81000000, chr17 becomes 17)
	 * @param	refName 
	 * 				Reference Name 
	 * @return  refName
	 * 				Reference Name  w/o "chr"	   
	 */
	
	private static String canonicalizeRefName(String refName) {
		return refName.replace("chr", "");
	}

	
	/**
	 * <h1>This function is the main function that creates and calls dataflow pipeline 
	 */
	public static void run(String[] args) throws Exception {
		// Register the options so that they show up via --help
		PipelineOptionsFactory.register(Options.class);
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		// Option validation is not yet automatic, we make an explicit call
		// here.
		Options.Methods.validateOptions(options);

		// Set up the prototype request and auth.
		StreamVariantsRequest prototype = CallSetNamesOptions.Methods.getRequestPrototype(options);
		OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
		Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);

		//check whether user provided VariantSetId 
		if (options.getVariantSetId().isEmpty()) {
			throw new IllegalArgumentException(
					"VaraiantSetIds must be specified (e.g., 10473108253681171589 for 1000 Genomes.)");
		}

		//check whether user provided CallSetNames 
		if (options.getCallSetNames().isEmpty()) {
			throw new IllegalArgumentException("CallSetNames must be specified (e.g., HG00261 for 1000 Genomes.)");
		}
		
		//Add Genotype field to the VCF HEADER! 
		String callSetNames="genotype(s):";
		for(String name:options.getCallSetNames().split(","))
		{
				callSetNames += name + ";";
		}
		HEADER += "\t" + callSetNames;
		
		
		if (options.getVariantAnnotationSetIds().isEmpty() && options.getTranscriptSetIds().isEmpty()) {
			throw new IllegalArgumentException(
					"Both VaraiantAnnotationSetIds/TranscriptSetIds are empty! At least one of them must be specified."
							+ "(e.g., CIjfoPXj9LqPlAEQ5vnql4KewYuSAQ for UCSC refGene (hg19) and CILSqfjtlY6tHxC0nNH-4cu-xlQ for ClinVar (GRCh37))");
		}


		List<String> callSetIds = CallSetNamesOptions.Methods.getCallSetIds(options);

		List<String> variantAnnotationSetIds = null;
		if (!Strings.isNullOrEmpty(options.getVariantAnnotationSetIds())) {
			variantAnnotationSetIds = validateAnnotationSetsFlag(genomics, options.getVariantAnnotationSetIds(),
					"VARIANT");
			prepareHeaderAnnotationSets(genomics, variantAnnotationSetIds, 5, "VARIANT"); 
			 /*
			  * The first five columns are redundant 
			  * [referenceName, start, end, referenceBases, alternateBases] are provided by variants
			  * */
		}
		
		List<String> transcriptSetIds = null;
		// TODO: Transcript and Generic should be separated
		if (!Strings.isNullOrEmpty(options.getTranscriptSetIds())) {
			transcriptSetIds = validateAnnotationSetsFlag(genomics, options.getTranscriptSetIds(), "GENERIC");
			prepareHeaderAnnotationSets(genomics, transcriptSetIds, 3, "GENERIC"); 
			 /*
			  * The first three columns are redundant - 
			  * [referenceName, start, end, referenceBases, alternateBases] are provided by variants
			  */
		}

		List<StreamVariantsRequest> requests = options.isAllReferences()
				? ShardUtils.getVariantRequests(prototype, ShardUtils.SexChromosomeFilter.INCLUDE_XY,
						options.getBasesPerShard(), auth)
				: ShardUtils.getVariantRequests(prototype, options.getBasesPerShard(), options.getReferences());

				
		System.out.println("Max Num of Extensible Columns: " + getNumCols());
		System.out.println("ChrM: " + options.getSupportChrM());
		
		
		//Here is the dataflow pipeline 
		Pipeline p = Pipeline.create(options);
		p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

		p.begin().apply(Create.of(requests))
				.apply(ParDo
						.of(new AnnotateVariants(auth, callSetIds, transcriptSetIds,
								variantAnnotationSetIds, ColInfoVariantAnnotation, 
								ColInfoTranscript, options.getSupportChrM(), options.getOnlySNP())))
				.apply(GroupByKey.<String, String> create())
				.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, String>() {
					@Override
					public void processElement(ProcessContext c) {
						if (!DEBUG) {
							String rows = "";
							int index=0;
							for (String s : c.element().getValue()){
								index++;
								if(index>1)
									rows += "\n" + s;
								else
									rows += s;
							}
							c.output(rows);
						} else {
							c.output(c.element().getKey() + ": " + c.element().getValue());
						}
					}
				})).apply(TextIO.Write.to(options.getOutput()));		
		p.run();
		
		
		Path VCF_Filename = Paths.get( options.getOutput());
		System.out.println("");
		System.out.println("");
		System.out.println("[INFO] ------------------------------------------------------------------------");
		System.out.println("[INFO] Below is the header of " + VCF_Filename.getFileName().toString());
		System.out.println(HEADER);
		System.out.println();
		System.out.println("[INFO] To check the current status of your job, use the following command:");
		System.out.println("\t ~: gcloud alpha dataflow jobs describe $YOUR_JOB_ID$");
		System.out.println("");
		System.out.println("[INFO] To gather the results into a single VCF file after the completion of job (i.e., currentState: JOB_STATE_DONE), run the following command:");
		System.out.println("\t ~: gsutil cat " + options.getOutput() + "* | sort > " + VCF_Filename.getFileName().toString());	
		System.out.println("");
		System.out.println("[INFO] Please add the header to " + VCF_Filename.getFileName().toString());
		System.out.println("[INFO] ------------------------------------------------------------------------");
		System.out.println("");
		System.out.println("");

		//TODO:Sort Output Files Using Dataflow

		
	}

	private static String formatTabs(int num) {
		String output = "";
		for (int i = 0; i < num; i++)
			output += "\t";
		return output;
	}


	/**
	 * <h1> This method prepares the header for the output VCF file
	 * 
	 * @param	genomics
	 * @param	annosetIds
	 * 			This is the input annotaionIds
	 * @param	redundant
 	 * @param	annotationType
	 */
	
	private static void prepareHeaderAnnotationSets(Genomics genomics, List<String> annosetIds, int redundant, String annotationType)
			throws IOException {

		for (String annosetId : annosetIds) {
			Map<String, List<Object>> gotInfo = genomics.annotationsets().get(annosetId).execute().getInfo();
			List<Object> objectValue = gotInfo.get("header");

			if (objectValue != null) {
				String [] fields = objectValue.get(0).toString().split(",");
				
					for(int index=redundant; index<fields.length; index++){
						HEADER += "\t" + fields[index];
					}

				addColInfo(annosetId, fields.length - redundant, annotationType);
			} else {
				throw new IllegalArgumentException("No header found for the annotionSet " + annosetId);
			}

		}
	}

	
	/**
	 * <h1> This method checks whether the types of annotationSets and variantSet are
	 * the same or not. In the case of matched, it will return back a list of annotionIds
	 * 
	 * @param	genomics
	 * @param	flagValue
	 * 			This is the input annotaionIds
	 * @param	wantType
	 * @return annosetIds
	 */
	private static List<String> validateAnnotationSetsFlag(Genomics genomics, String flagValue, String wantType)
			throws IOException {
		List<String> annosetIds = ImmutableList.copyOf(flagValue.split(","));
		for (String annosetId : annosetIds) {
			AnnotationSet annoset = genomics.annotationsets().get(annosetId).execute();
			if (!wantType.equals(annoset.getType())) {
				throw new IllegalArgumentException(
						"annotation set " + annosetId + " has type " + annoset.getType() + ", wanted type " + wantType);
			}
		}
		return annosetIds;
	}
	
	
	/**
	 * <h1>This function visits all input annotation sets, and calculate the maximum number of extended columns.  
	 * @return NumCols
	 * 		The function returns back the maximum number of columns that will be added after the annotation to the VCF file. 
	 */
	
	public static int getNumCols() {
		int NumCols = 0;

		for (Map.Entry<String, Integer> entry : ColInfoTranscript.entrySet()) {
			String key = entry.getKey();
			int value = entry.getValue();
			System.out.println("Transcript AnnotationSetId: " + key + "\t Number of Columns: " + value);
			NumCols += value;
		}
		
		for (Map.Entry<String, Integer> entry : ColInfoVariantAnnotation.entrySet()) {
			String key = entry.getKey();
			int value = entry.getValue();
			System.out.println("Variant AnnotationSetId: " + key + "\t Number of Columns: " + value);
			NumCols += value;
		}
		return NumCols;
	}
	
	/**
	 * <h1>This function simply adds information <AnnotationSetId, Max. number of extended columns>. The information is used to aligned VCF columns.    
	 */

	public static void addColInfo(String asId, int numCols, String type) {
		if (type.equalsIgnoreCase("VARIANT"))
			ColInfoVariantAnnotation.put(asId, numCols);
		else if (type.equalsIgnoreCase("TRANSCRIPT") || type.equalsIgnoreCase("GENERIC"))
			ColInfoTranscript.put(asId, numCols);
	}
	
	
}