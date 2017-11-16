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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Logger;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;

import org.apache.beam.sdk.io.gcp.bigquery.*; 
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Ordering; 
import com.google.common.base.Function; 

public final class BigQueryAnnotateVariants {
	
	public static interface Options extends GenomicsOptions
	{
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

		
		@Description("This provides the path to the local output file.")
		@Default.String("")
		String getLocalOutputFilePath();
		void setLocalOutputFilePath(String LocalOutputFilePath);

		@Description("This provides the address for reference of VCF tables. ")
		@Default.String("")
		String getVCFTables();
		void setVCFTables(String VCFTables);
		
		@Description("This provides the Project ID. ")
		@Default.String("")
		String getProjectId();
		void setProjectId(String ProjectId);

		@Description("This provides the Output File. ")
		@Default.String("")
		String getOutput();
		void setOutput(String Output);

		@Description("This provides the address of Generic Annotation tables.")
		@Default.String("")
		String getGenericAnnotationTables();
		void setGenericAnnotationTables(String GenericAnnotationTables);

		@Description("This provides the address of Variant Annotation tables.")
		@Default.String("")
		String getVariantAnnotationTables();
		void setVariantAnnotationTables(String VariantAnnotationTables);
		
		@Description("This provides the prefix for reference field in VCF tables (e.g, \"chr\")")
		@Default.String("")
		String getVCFCanonicalizeRefNames();
		void setVCFCanonicalizeRefNames(String VCFCanonicalizeRefNames);

		@Description("This provides the prefix for reference field in Transcript tables (e.g, \"chr\")")
		@Default.String("")
		String getTranscriptCanonicalizeRefNames();
		void setTranscriptCanonicalizeRefNames(String TranscriptCanonicalizeRefNames);

		@Description("This provides the prefix for reference field in Annotation tables (e.g, \"chr\")")
		@Default.String("")
		String getVariantAnnotationCanonicalizeRefNames();
		void setVariantAnnotationCanonicalizeRefNames(String VariantAnnotationCanonicalizeRefNames);

	    @Description("Genomic window \"bin\" size to use")
	    @Default.Integer(1000000)
	    int getBinSize();
	    void setBinSize(int BinSize);

	    @Description("User can decide using Dataflow or BigQuery for Sorting")
		@Default.Boolean(false)
		boolean getBigQuerySort();
		void setBigQuerySort(boolean BigQuerySort);   
	        		
	}

	private static Options options;
	private static Pipeline p;
	//private static OfflineAuth auth;
	private static final Logger LOG = Logger.getLogger(BigQueryAnnotateVariants.class.getName());


	/**
	 * <h1>This function is the main function that creates and calls dataflow
	 * pipeline
	 */
	public static void run(String[] args) throws GeneralSecurityException, IOException  {
			
		
		//TODO: Make sure all VCF files and annotation sets are in the same systems (e.g., hg19/GRCh37 or hg20/GRCh38) 

		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
				
		// Here is the dataflow pipeline
		p = Pipeline.create(options);

		
		// check whether user provided VariantSetId
		if (options.getGenericAnnotationTables().isEmpty() && options.getVariantAnnotationTables().isEmpty()) {
			throw new IllegalArgumentException("Please specify at least one annotation table. ( e.g., VariantAnnotationTables=silver-wall-555:TuteTable.hg19");
		}

		// check whether user provided CallSetNames
		if (options.getVCFTables().isEmpty()) {
			throw new IllegalArgumentException("Please specify VCF tables (e.g., VCFTables=genomics-public-data:platinum_genomes.variants )");
		}

	
	String queryString = BigQueryFunctions.prepareAnnotateVariantQuery(options.getVCFTables(), options.getVCFCanonicalizeRefNames(), options.getGenericAnnotationTables(), 
			options.getTranscriptCanonicalizeRefNames() , options.getVariantAnnotationTables(), options.getVariantAnnotationCanonicalizeRefNames());
	
	System.out.println("Query: " + queryString);

	
	
	//////////////////////////////////STEP1: Run Joins/////////////////////////////////////  
	long startTime = System.currentTimeMillis();
	
	try {
		BigQueryFunctions.runQueryPermanentTable(queryString, options.getBigQueryDataset(), 
				options.getBigQueryTable(), true);
	} catch (TimeoutException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	long tempEstimatedTime = System.currentTimeMillis() - startTime;
	System.out.println("Execution Time for Join Query: " + tempEstimatedTime);
	//////////////////////////////////////////////////////////////////////////////////////	

	
	//////////////////////////////////STEP2: Sort/////////////////////////////////////  
	startTime = System.currentTimeMillis();
	
	
	if(!options.getBigQuerySort()){
	
		//////////////////////////////////STEP2: Run Dataflow Sort/////////////////////////////////////  
		PCollection<TableRow> variantData = p.apply(BigQueryIO.read().fromQuery(
				"SELECT  * FROM [" + options.getProjectId() +":"+ options.getBigQueryDataset()+"."+ options.getBigQueryTable() +"]"));
	
		PCollection<KV<Long, TableRow>> variantDataKV = variantData.apply(ParDo.of(new BinVariantsFn()));
		
		//GroupBy each Bin and then sort it
		PCollection<KV<Integer, KV<Long, Iterable<String>>>> SortedBin = 
				variantDataKV.apply("Group By Variants Based on BinID", GroupByKey.<Long, TableRow>create())
		
		//Sort bins in parallel		
				.apply("Sort Variants inside Bins", ParDo.of(
			      new DoFn<KV<Long, Iterable<TableRow>>, KV<Integer, KV<Long, Iterable<String>>>>() {
			         
					private static final long serialVersionUID = 803837233177187278L;
	
				    @ProcessElement 
			          public void processElement(ProcessContext c) {
						
			            KV<Long, Iterable<TableRow>> e = c.element();
			            Long key = e.getKey();
			            		            
					    List<TableRow> records = Lists.newArrayList(e.getValue());  // Get a modifiable list.
						Collections.sort(records, VARIANT_SEGMENT_COMPARATOR);
						//List<String> recordsString = Lists.newArrayList();

						//TODO: Merge similar results && convert to 1-base
						
						//for (TableRow T:records){
						//	recordsString.add(T.values().toString());
						//}

						////////////////////////
						
						//TableRow row = null;
						// Object[] val1 = row.values().toArray();
						List<String> mergedItems=Lists.newArrayList(); //= recordsString;
						String buffer="";
						TableRow oldRow=null;
						for (TableRow row : records) {
							String temp="";
							Object[] fieldValues = row.values().toArray();
							for (int index=0; index<fieldValues.length; index++) {
									    	if(fieldValues[index]!=null){
									    		if(index==1) //Start
									    		{
									    			long tempStart = Long.parseLong(fieldValues[index].toString()) + 1;
									    			temp += tempStart + "\t" ;
									    		}
									    		else
									    			temp += fieldValues[index].toString() + "\t";
									    	}
							}
							if(oldRow==null)
							    buffer = temp;
							else if (oldRow!=null){  
								      if (row.get("start").toString().equals(oldRow.get("start").toString()) &&
									    			row.get("end").toString().equals(oldRow.get("end").toString()) &&
									    			row.get("alternate_bases").toString().equals(oldRow.get("alternate_bases").toString()	)){
								    	  
										    		for (int index=5; index<fieldValues.length; index++) {    	  
												 	if(fieldValues[index]!=null)
												    	  	buffer += fieldValues[index].toString() + "\t";
												}
									    	}
									    	else{
									    		mergedItems.add(buffer);
										    buffer = temp;
									    	}
								 }
	
								 oldRow = row;
						}
						mergedItems.add(buffer);
						
						
						
						Iterable<String> sortedBin = mergedItems; //recordsString;
						
						int chrm = (int) (key/1000000000);
												
						c.output(KV.of(chrm, KV.of(key,  sortedBin)));
					}		            
			        
			      }));
		
		PCollection<KV<Integer, KV<Integer, Iterable<KV<Long, Iterable<String>>>>>> SortedChrm = 
			SortedBin.apply("Group By BinID",GroupByKey.<Integer, KV<Long,Iterable<String>>>create())			
				.apply("Sort By BinID ",ParDo.of(
			      new DoFn<KV<Integer, Iterable<KV<Long, Iterable<String>>>>, KV<Integer, KV<Integer, Iterable<KV<Long, Iterable<String>>>>>>() {
			          
	
					private static final long serialVersionUID = -8017322538250102739L;
					
					@org.apache.beam.sdk.transforms.DoFn.ProcessElement
			          public void processElement(ProcessContext c) {
						
			            KV<Integer, Iterable<KV<Long, Iterable<String>>>> e = c.element();
			            Integer Secondary_key = e.getKey();
			        
			            
			            ArrayList<KV<Long, Iterable<String>>> records = Lists.newArrayList(e.getValue());  // Get a modifiable list.
			            LOG.warning("Total Mem: " + Runtime.getRuntime().totalMemory());
			            LOG.warning("Free Mem: " + Runtime.getRuntime().freeMemory() );
			            
			            Collections.sort(records, BinID_COMPARATOR);
	
						Integer Primary_key=1;
						c.output(KV.of(Primary_key, KV.of(Secondary_key, (Iterable<KV<Long, Iterable<String>>>) records)));
						
					}		            
			      }));
		
		
		
		SortedChrm.apply("Group By Chromosome",GroupByKey.<Integer, KV<Integer, Iterable<KV<Long,Iterable<String>>>>>create())
		.apply("Sort By Chromosome" ,ParDo.of(
				new DoFn<KV<Integer, Iterable<KV<Integer, Iterable<KV<Long, Iterable<String>>>>>>, String>() {
					private static final long serialVersionUID = 403305704191115836L;
	
					@org.apache.beam.sdk.transforms.DoFn.ProcessElement
			          public void processElement(ProcessContext c) {
								        
			             ArrayList<KV<Integer, Iterable<KV<Long, Iterable<String>>>>> records = Lists.newArrayList(c.element().getValue());  // Get a modifiable list.
						 Collections.sort(records, ChrmID_COMPARATOR);
					          
						 for(KV<Integer, Iterable<KV<Long, Iterable<String>>>> ChromLevel:records){
							 for( KV<Long, Iterable<String>> BinLevel:ChromLevel.getValue()){
	
								 for(String ItemLevel:BinLevel.getValue()){
									    /////////////////////////
							 			c.output(ItemLevel);
								 }
							 }
						 }
					}		            
			      })).apply("VCF", TextIO.write().to(options.getOutput()));
	
	
		
	    p.run().waitUntilFinish();
		
		 Path VCF_Filename = Paths.get( options.getOutput());
		 System.out.println("");
		 System.out.println("");
		 System.out.println("[INFO]------------------------------------------------------------------------");
		 System.out.println("[INFO] To download the annotated VCF file from Google Cloud, run the following command:");
		 System.out.println("\t ~: gsutil cat " + options.getOutput() + "* > " + VCF_Filename.getFileName().toString());
		 System.out.println("");
		 System.out.println("[INFO] To remove the output files from the cloud storage run the following command:");
		 System.out.println("\t ~: gsutil rm " + options.getOutput() + "*");
		 System.out.println("[INFO] ------------------------------------------------------------------------");
		 System.out.println("");
		 System.out.println("");

	}
	else{

		//////////////////////////////////STEP2: Run BigQuery Sort/////////////////////////////////////  
		try {
			BigQueryFunctions.sortByBin(options.getProject(), options.getBigQueryDataset(), 
					options.getBigQueryTable(), options.getLocalOutputFilePath(), options.getBinSize());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//////////////////////////////////////////////////////////////////////////////////////

		
	}
	
		
	tempEstimatedTime = System.currentTimeMillis() - startTime;
	System.out.println("Execution Time for Sort Query: " + tempEstimatedTime);
	//////////////////////////////////////////////////////////////////////////////////////

	
	
	//////////////////////////////STEP3: Delete the Intermediate Table ///////////////////  
	BigQueryFunctions.deleteTable(options.getBigQueryDataset(), options.getBigQueryTable());
	
	}
	
	 static final class BinVariantsFn extends DoFn<TableRow, KV<Long, TableRow>> {
	     /**
		 * 
		 */
		private static final long serialVersionUID = -6022090656022962093L;

		public static final long getStartBin(int binSize, TableRow rowVariant) {
	       // Round down to the nearest integer
	       return Math.round(Math.floor(Integer.parseInt(rowVariant.get("start").toString()) / binSize));
	     }

	     public static final long getEndBin(int binSize, TableRow rowVariant) {
	       // Round down to the nearest integer
	       return Math.round(Math.floor(Integer.parseInt(rowVariant.get("end").toString()) / binSize));
	     }

	     @ProcessElement
	     public void processElement(ProcessContext context) {
	       Options options =
	           context.getPipelineOptions().as(Options.class);
	       int binSize = options.getBinSize();
	       TableRow rowVariant = context.element();
	       long startBin = getStartBin(binSize, rowVariant);
	   
	        String key = rowVariant.get("chrm").toString();
	        if(key.equalsIgnoreCase("Y"))
	        		key = "23";
	        else 	if (key.equalsIgnoreCase("X"))
	        		key ="24";
	        else if (key.equalsIgnoreCase("M") || key.equalsIgnoreCase("MT") )
	        		key="25";
	        
	        	String BinNum = Long.toString(startBin);
	        int numZeros = 9 - BinNum.length();
	   
	        //Padding
	        for(int index=1; index<=numZeros; index++){
	        		key += "0"; // Shift ChrmID
	        }
	        key += BinNum;
	        
	   
	        context.output(KV.of(Long.parseLong(key), rowVariant));

	       
	//       }
	     }
	   }
	 
	  // Special-purpose comparator for use in dealing with both variant and non-variant segment data. 
	  // Sort by start position ascending and ensure that if a variant and a ref-matching block are at 
	  // the same position, the non-variant segment record comes first. 
	 private static final Ordering<TableRow> BY_START = Ordering.natural().onResultOf( 
		      new Function<TableRow, Long>() { 
		        @Override 
		        public Long apply(TableRow variant) { 
		          return Long.parseLong(variant.get("start").toString()); 
		        } 
		      });
	 
	 private static final Ordering<TableRow> BY_FIRST_OF_ALTERNATE_BASES = Ordering.natural() 
		      .nullsFirst().onResultOf(new Function<TableRow, String>() { 
		        @Override 
		        public String apply(TableRow variant) { 
		          if (null == variant.get("alternate_bases").toString() || variant.get("alternate_bases").toString().isEmpty()) { 
		            return null; 
		          } 
		          return variant.get("alternate_bases").toString(); 
		        } 
		      });
	 
	 static final Comparator<TableRow> VARIANT_SEGMENT_COMPARATOR = BY_START 
	      .compound(BY_FIRST_OF_ALTERNATE_BASES); 

	 
	 
	 private static final Ordering<KV<Long, Iterable<String>>> BY_BinID = Ordering.natural().onResultOf( 
		      new Function<KV<Long, Iterable<String>>, Long>() { 
		        @Override 
		        public Long apply(KV<Long, Iterable<String>> variant) { 
		          return Long.parseLong(variant.getKey().toString()); 
		        } 
		      });
	 
	 static final Comparator<KV<Long, Iterable<String>>> BinID_COMPARATOR = BY_BinID; 


	 private static final Ordering<KV<Integer, Iterable<KV<Long, Iterable<String>>>>> BY_ChrmID = Ordering.natural().onResultOf( 
		      new Function<KV<Integer, Iterable<KV<Long, Iterable<String>>>>, Integer>() { 
		        @Override 
		        public Integer apply(KV<Integer, Iterable<KV<Long, Iterable<String>>>> variant) { 
		          return Integer.parseInt(variant.getKey().toString()); 
		        } 
		      });
	 
	 static final Comparator<KV<Integer, Iterable<KV<Long, Iterable<String>>>>> ChrmID_COMPARATOR = BY_ChrmID; 
  

}


