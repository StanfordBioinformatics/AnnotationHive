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
import com.google.api.services.genomics.model.ListBasesResponse;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.genomics.v1.StreamVariantsRequest;
import com.google.genomics.v1.StreamVariantsResponse;
import com.google.genomics.v1.Variant;

import htsjdk.samtools.util.IntervalTree;
//import htsjdk.samtools.util.IntervalTree.Node;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * <h1>Annotating Google Genomics Variant Sets</h1> This class  an annotationSet, and imports annotations.
 * It prepares and converts Annotation fields to the corresponding fields in the
 * Google Genomics annotation set APIs.
 * 
 * @param variantSetId
 *            The ID of the Google Genomics variant set this pipeline is accessing.
 * @param callSetNames
 *            This provides the name for the AnnotationSet.
 * @param transcriptSetIds
 *            The IDs of the Google Genomics transcript sets.
 * @param variantAnnotationSetIds          
 * 			  The IDs of the Google Genomics variant annotation sets.
 * @version 1.0
 * @since 2016-07-01
 */

final class AnnotateVariants extends DoFn<StreamVariantsRequest, KV<String, String>> {

  public static interface Options extends
    // Options for call set names.
    CallSetNamesOptions,
    // Options for calculating over regions, chromosomes, or whole genomes.
    ShardOptions,
    // Options for the output destination.
    GCSOutputOptions {

    @Override
    @Description("The ID of the Google Genomics variant set this pipeline is accessing. "
        + "Defaults to 1000 Genomes.")
    @Default.String("10473108253681171589")
    String getVariantSetId();

    @Override
    @Description("The names of the Google Genomics call sets this pipeline is working with, comma "
        + "delimited.  Defaults to 1000 Genomes HG00261.")
    @Default.String("HG00261")
    String getCallSetNames();

    @Description("The IDs of the Google Genomics variant annotation sets this pipeline is working "
        + "with, comma delimited. Defaults to ClinVar (GRCh37).")
    @Default.String("CILSqfjtlY6tHxC0nNH-4cu-xlQ")
    String getVariantAnnotationSetIds();
    void setVariantAnnotationSetIds(String variantAnnotationSetIds);
    

    public static class Methods {
      public static void validateOptions(Options options) {
        GCSOutputOptions.Methods.validateOptions(options);
      }
    }
  }

  private static final Logger LOG = Logger.getLogger(AnnotateVariants.class.getName());
  private static final String VARIANT_FIELDS
      = "variants(id,referenceName,start,end,alternateBases,referenceBases)";

  private final OfflineAuth auth;
  private final List<String> callSetIds, variantAnnotationSetIds;

  public AnnotateVariants(OfflineAuth auth,
      List<String> callSetIds, 
      List<String> variantAnnotationSetIds) {
    this.auth = auth;
    this.callSetIds = callSetIds;
    this.variantAnnotationSetIds = variantAnnotationSetIds;
  }

  @Override
  public void processElement(
      DoFn<StreamVariantsRequest, KV<String, String>>.ProcessContext c) throws Exception {
    Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);

    StreamVariantsRequest request = StreamVariantsRequest.newBuilder(c.element())
        .addAllCallSetIds(callSetIds)
        .build();
    LOG.info("processing contig " + request);

    Iterator<StreamVariantsResponse> iter = VariantStreamIterator.enforceShardBoundary(auth,
        request, ShardBoundary.Requirement.STRICT, VARIANT_FIELDS);
    if (!iter.hasNext()) {
      LOG.info("region has no variants, skipping");
      return;
    }

    if(!"M".equals(request.getReferenceName())){
	    ListMultimap<Range<Long>, Annotation> variantAnnotations =
	        retrieveVariantAnnotations(genomics, request);
	
	    Stopwatch stopwatch = Stopwatch.createStarted();
	    int varCount = 0;
	    while (iter.hasNext()) {
	      Iterable<Variant> varIter = FluentIterable
	          .from(iter.next().getVariantsList())
	          .filter(VariantUtils.IS_SNP);
	      for (Variant variant : varIter) {   	  
	        Range<Long> pos = Range.closedOpen(variant.getStart(), variant.getEnd());
	        
	       	for (Annotation match : variantAnnotations.get(pos)) {
	       		String output = createVCFFormat(variant, match);
	       		c.output(KV.of(variant.getReferenceName() , output ));
	       	}
	       	     	
	        varCount++;
	        if (varCount%1e3 == 0) {
	          LOG.info(String.format("read %d variants (%.2f / s)",
	              varCount, (double)varCount / stopwatch.elapsed(TimeUnit.SECONDS)));
	        }
	      }
	    }
	    LOG.info("finished reading " + varCount + " variants in " + stopwatch);
	    }
  }

  private String createVCFFormat(Variant variant, Annotation match) {
 		/*VCF format: #CHROM POS    ID        REF  ALT     QUAL FILTER INFO       
 		 * FORMAT      Sample1        Sample2        Sample3*/ 
	  
	  /*Add variant's alternative bases*/
	  String VALTs="";
	  for (int index=0; index < variant.getAlternateBasesCount(); index++)
		  VALTs +=  variant.getAlternateBases(index);
	
	  /*Add variant's filters*/
	  String VFilters="";
	  for (int index=0; index < variant.getFilterCount(); index++)
		  VFilters +=  variant.getFilter(index);
	  
	  String AnnotationMap="";
	  
	  /*Sorting Annotation Info based on key*/
	  SortedSet<String> keys = new TreeSet<String>( match.getInfo().keySet());
	  for (String key : keys) { 
	     String stringValues="";
		 List<Object> objectValue = match.getInfo().get(key);
	     for(Object value: objectValue)
	    	 stringValues += value.toString();
		 AnnotationMap += key +"="+ stringValues + ";";
	  }
	  
	  /*create a row of the output VCF file*/
	  String output = variant.getReferenceName() 
			  + "\t" + variant.getStart()
			  + "\t" + variant.getId() 
			  + "\t" + variant.getReferenceBases() 
			  + "\t" + VALTs 
			  + "\t" + variant.getQuality() 
			  + "\t" + VFilters 
			  + "\t" + AnnotationMap;
	   
	  return output;
}
  
private ListMultimap<Range<Long>, Annotation> retrieveVariantAnnotations(
      Genomics genomics, StreamVariantsRequest request) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    ListMultimap<Range<Long>, Annotation> annotationMap = ArrayListMultimap.create();
    Iterable<Annotation> annotationIter =
        Paginator.Annotations.create(genomics, ShardBoundary.Requirement.OVERLAPS).search(
            new SearchAnnotationsRequest()
              .setAnnotationSetIds(variantAnnotationSetIds)
              .setReferenceName(canonicalizeRefName(request.getReferenceName()))
              .setStart(request.getStart())
              .setEnd(request.getEnd()));
    if(annotationIter != null){
	    for (Annotation annotation : annotationIter) {
	      long start = 0;
	      if (annotation.getStart() != null) {
	        start = annotation.getStart();
	      }
	      annotationMap.put(Range.closedOpen(start, annotation.getEnd()), annotation);
	    }
    }
    else
    	LOG.warning("There is no annotation for RefernceName: " + request.getReferenceName() + " in the input Annotation Set (annotationSetId: " + request.getVariantSetId() +")");
    
    LOG.info(String.format("read %d variant annotations in %s (%.2f / s)", annotationMap.size(),
        stopwatch, (double)annotationMap.size() / stopwatch.elapsed(TimeUnit.SECONDS)));
    return annotationMap;
  }

  

  private static String canonicalizeRefName(String refName) {
    return refName.replace("chr", "");
  }

  public static void run(String[] args) throws Exception {
    // Register the options so that they show up via --help
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(Options.class);
    // Option validation is not yet automatic, we make an explicit call here.
    Options.Methods.validateOptions(options);

    // Set up the prototype request and auth.
    StreamVariantsRequest prototype = CallSetNamesOptions.Methods.getRequestPrototype(options);
    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    Genomics genomics = GenomicsFactory.builder().build().fromOfflineAuth(auth);

    
    List<String> callSetIds = CallSetNamesOptions.Methods.getCallSetIds(options);
    if (callSetIds.isEmpty()) 
    	System.out.println("Empty");
    
    	System.out.println(callSetIds.toString());


    List<String> variantAnnotationSetIds =
        validateAnnotationSetsFlag(genomics, options.getVariantAnnotationSetIds(), "GENERIC");

    //TODO: Customize the request based on the content of variantSet. 
    List<StreamVariantsRequest> requests = options.isAllReferences() ?
        ShardUtils.getVariantRequests(prototype, ShardUtils.SexChromosomeFilter.EXCLUDE_XY,
            options.getBasesPerShard(), auth) :
              ShardUtils.getVariantRequests(prototype, options.getBasesPerShard(), options.getReferences());

    Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().setFallbackCoderProvider(GenericJsonCoder.PROVIDER);

    p.begin()
      .apply(Create.of(requests))
      .apply(ParDo.of(new AnnotateVariants(auth, callSetIds, variantAnnotationSetIds)))
      .apply(GroupByKey.<String, String>create())
      .apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, String>() 
      {
		@Override
        public void processElement(ProcessContext c) {
		
			String rows="";
			for (String s: c.element().getValue())
				rows += s + "\n";
			c.output(rows);
        }
      }))
      .apply(TextIO.Write.to(options.getOutput()));
    p.run();
  }

	/**
	 * This method checks whether the types of annotationSets and variantSet are the same or not
	 */
  private static List<String> validateAnnotationSetsFlag(
      Genomics genomics, String flagValue, String wantType) throws IOException {
    List<String> annosetIds = ImmutableList.copyOf(flagValue.split(","));
    for (String annosetId : annosetIds) {
      AnnotationSet annoset = genomics.annotationsets().get(annosetId).execute();
      if (!wantType.equals(annoset.getType())) {
        throw new IllegalArgumentException("annotation set " + annosetId + " has type " +
            annoset.getType() + ", wanted type " + wantType);
      }
    }
    return annosetIds;
  }
}