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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Ordering;
import com.google.common.base.Function;

/**
 * <h1>Annotating Using BigQuery</h1> This class helps to Annotates BigQuery
 * mVCF tables. It automatically populates BigQuery queries. In case, users want
 * to get output as annotated VCF file (not table), this sort function sorts the
 * output of BigQuery in Dataflow. Default sorting algorithm is Dataflow.
 * 
 * @param projectID
 *            The ID of the Google Cloud project.
 * @param bigQueryDatasetId
 *            The ID of the user's BigQuery dataset.
 * @param outputBigQueryTable
 *            This provides the name for the table that will be created by
 *            BigQuery for Annotated mVCF file.
 * @param binSize
 *            Sorting algorithm partitions VCF into bins and start sorting these
 *            bins in a bottom-up approach (Default value is 1000000).
 * @param VCFTables
 *            The IDs of the mVCF/VCF table on BigQuery.
 * @param sampleId
 *            If you are interested in only a sample of mVCF table, then you can
 *            specify the sample ID, and AnnotaionHive only considers this
 *            sampleId.
 * @param bucketAddrAnnotatedVCF
 *            The Cloud storage address of the output annotated VCF file.
 * @param genericAnnotationTables
 *            The IDs of generic annotation tables.
 * @param variantAnnotationTables
 *            The IDs of variant annotation tables.
 * @param VCFCanonicalizeRefNames
 *            Users can specify any prefix for reference field in VCF tables
 *            (e.g., \"chr\" for Platinum Genomes).            
 * @param geneBasedAnnotation
 *            To ask AnnotationHive to do gene-based annotation, users need to
 *            specify this variable true (Default value is false).
 * @param onlyIntrogenic
 *            Under Gene-based annotation, if you are only interested in
 *            introgenic variants, set this variable true (Default value is
 *            false).
 * @param proximityThreshold
 *            For gene-based annotation, if users want to find the regions to
 *            any variant, they can specify the proximity range (Default value
 *            is 1000).
 * @param geneBasedMinAnnotation
 *            For gene-based annotation, if users want to find the closest
 *            region to any variant, they must set this parameter true (Default
 *            value is false).
 * @param bigQuerySort
 *            Users can choose BigQuery to sort the output. Note, ‘Order By’ has
 *            an upper bound for the size of table it can sort, AnnotationHive
 *            dynamically partitions the output considering the number of
 *            annotated variants and then applies the Order By to each of those
 *            partitions.
 * @param localOutputFilePath
 *            specify this file when you want to sort the output of BigQuery
 *            using BigQuery itself.
 * @param maximumBillingTier
 *            Users can specify the maximum billing tier.
 * @param searchRegions  
 *            specify the regions of interest (i.e., chr13:32889610:32973808,chr17:41196311:41277499 ) 
 * @param tableExists
 * 			  If table exists, then set this true
 * @param deleteOutputTable
 * 			  If users want to delete the output table, they need to set this true
 * @param googleVCF
 * 			  If users have a VCF table imported using Google APIs; they need to set this true
 * @param createVCF
 * 			  If user wants to get a VCF file (true/false - default is false, and it creates a table)
 * @param numberSamples           
 * 			  If users have a VCF table imported using Google APIs, and they want to get the number of samples 
 * 		 from the multiple VCF file, then they need to set this true
 * @version 1.0
 * @since 2018-02-01
 */

public final class BigQueryAnnotateVariants {

	public static interface Options extends GenomicsOptions {

		@Description("This provides BigQuery Dataset ID.")
		@Default.String("")
		String getBigQueryDatasetId();

		void setBigQueryDatasetId(String BigQueryDatasetId);

		@Description("This provides BigQuery Table.")
		@Default.String("")
		String getOutputBigQueryTable();

		void setOutputBigQueryTable(String OutputBigQueryTable);

		@Description("This provides the path to the local output file.")
		@Default.String("")
		String getLocalOutputFilePath();

		void setLocalOutputFilePath(String LocalOutputFilePath);

		@Description("This provides the address for reference of VCF tables. ")
		@Default.String("")
		String getVCFTables();

		void setVCFTables(String VCFTables);

		@Description("This provides the prefix for reference field in VCF tables (e.g, \"chr\")")
		@Default.String("")
		String getVCFCanonicalizeRefNames();

		void setVCFCanonicalizeRefNames(String VCFCanonicalizeRefNames);

		@Description("This provides the Project ID. ")
		@Default.String("")
		String getProjectId();

		void setProjectId(String ProjectId);

		@Description("This provides the address to the output VCF File. ")
		@Default.String("")
		String getBucketAddrAnnotatedVCF();

		void setBucketAddrAnnotatedVCF(String BucketAddrAnnotatedVCF);

		@Description("This provides the address of Generic Annotation tables.")
		String getGenericAnnotationTables();
		void setGenericAnnotationTables(String GenericAnnotationTables);

		@Description("This provides the address of Generic Annotation tables with UCSC ID.")
		String getUCSCGenericAnnotationTables();
		void setUCSCGenericAnnotationTables(String UCSCGenericAnnotationTables);
		
		@Description("This provides the address of Variant Annotation tables.")
		String getVariantAnnotationTables();
		void setVariantAnnotationTables(String VariantAnnotationTables);

		@Description("This provides the prefix for reference field in Transcript tables (e.g, \"chr\")")
		@Default.String("")
		String getGenericCanonicalizeRefNames();

		void setGenericCanonicalizeRefNames(String GenericCanonicalizeRefNames);

		@Description("This provides the prefix for reference field in Annotation tables (e.g, \"chr\")")
		@Default.String("")
		String getVariantAnnotationCanonicalizeRefNames();

		void setVariantAnnotationCanonicalizeRefNames(String VariantAnnotationCanonicalizeRefNames);

		@Description("Genomic build (e.g., hg19, hg38)")
		@Default.String("")
		String getBuild();
		void setBuild(String Build);
		
		@Description("Genomic window \"bin\" size to use")
		@Default.Integer(1000000)
		int getBinSize();
		void setBinSize(int BinSize);

		@Description("Proximity Threshold (i.e., Number of bps) for Gene-based annotation")
		@Default.Integer(1000)
		int getProximityThreshold();

		void setProximityThreshold(int ProximityThreshold);

		@Description("The query pricing rate has levels called “billing tiers”.")
		@Default.Integer(1)
		int getMaximumBillingTier();

		void setMaximumBillingTier(int MaximumBillingTier);

		@Description("User can decide using Dataflow or BigQuery for Sorting")
		@Default.Boolean(false)
		boolean getBigQuerySort();

		void setBigQuerySort(boolean BigQuerySort);

		@Description("In gene-based annotation, if you only want Introgenic variants to be annotated")
		@Default.Boolean(false)
		boolean getOnlyIntrogenic();

		void setOnlyIntrogenic(boolean onlyIntrogenic);

		@Description("Gene-based Annotation Process")
		@Default.Boolean(false)
		Boolean getGeneBasedAnnotation();

		void setGeneBasedAnnotation(Boolean GeneBasedAnnotation);

		@Description("Gene-based Min Annotation Process")
		@Default.Boolean(false)
		Boolean getGeneBasedMinAnnotation();

		void setGeneBasedMinAnnotation(Boolean GeneBasedMinAnnotation);

		@Description("User can specifiy an input sample ID(e.g., callset.calset_name)")
		@Default.String("")
		String getSampleId();

		void setSampleId(String SampleId);

		@Description("If user wants to get a VCF file (true/false - default is false, and it creates a table)")
		@Default.Boolean(false)
		Boolean getCreateVCF();

		void setCreateVCF(Boolean createVCF);

		@Description("User can specifiy a variant in the form of \"Chromosome Id:start:end:Reference_bases:Alternate bases (e.g., chr17:1001:1001:A:C)\" )")
		@Default.String("")
		String getInputVariant();

		void setInputVariant(String value);

		@Description("User can specifiy a region in the form of \"Chromosome Id:start:end (e.g., chr17:1000:2000)\" )")
		@Default.String("")
		String getInputRegion();

		void setInputRegion(String value);
		
		@Description("specify the regions of interest (i.e., chr13:32889610:32973808,chr17:41196311:41277499 )")
		@Default.String("")
		String getSearchRegions();
		void setSearchRegions(String regions);
		
		@Description("If users want to delete the output table, they need to set this true")
		@Default.Boolean(false)
		boolean getDeleteOutputTable();
		void setDeleteOutputTable(boolean deleteOutputTable);

		@Description("If table exists, then set this true")
		@Default.Boolean(false)
		boolean getTableExists();
		void setTableExists(boolean tableExists);

		@Description("If Google generated the VCF table, then set this true")
		@Default.Boolean(false)
		boolean getGoogleVCF();
		void setGoogleVCF(boolean googleVCF);

		@Description("If Google generated the VCF table and want to count the number of samples, then you can set this true")
		@Default.Boolean(false)
		boolean getNumberSamples();
		void setNumberSamples(boolean numberSamples);
	}

	private static Options options;
	private static Pipeline p;
	private static final Logger LOG = Logger.getLogger(BigQueryAnnotateVariants.class.getName());

	/**
	 * <h1>This function is the main function that populates queries and calls
	 * dataflow/BigQuering sorting
	 */
	public static void run(String[] args) {

		// TODO: Make sure all VCF files and annotation sets are in the same
		// systems (e.g., hg19/GRCh37 or hg20/GRCh38)

		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		// Here is the dataflow pipeline
		p = Pipeline.create(options);

		
		//TODO: check if builds are the same!!!!
		LOG.warning("NOTE: Build is " + options.getBuild());
		if (!options.getBuild().equalsIgnoreCase("hg19") && !options.getBuild().equalsIgnoreCase("hg38")) {
			throw new IllegalArgumentException(
					"Currently, AnnotationHive only supports hg19 and hg38");
		}
		
		// check whether user provided any generic or variant annotation table IDs
		if (options.getGenericAnnotationTables() == null && options.getVariantAnnotationTables() == null && 
				options.getUCSCGenericAnnotationTables()== null) {
			throw new IllegalArgumentException(
					"Please specify at least one annotation table. ( e.g., VariantAnnotationTables=silver-wall-555:TuteTable.hg19");
		}
		// Check if users want to do gene-base. In the case of gene-based annotation,
		// users can only specify one generic annotation
		if (options.getGeneBasedAnnotation()) {

			if (options.getGenericAnnotationTables() == null || (options.getVariantAnnotationTables() != null))
				throw new IllegalArgumentException(
						"Please specify a gene-based annotation table. ( e.g., UCSC_refGene_hg19");
			if (options.getGenericAnnotationTables().split(",").length > 1) {
				throw new IllegalArgumentException(
						"Please specify ONE gene-based annotation table. ( e.g., UCSC_refGene_hg19");
			}
		}

		if (options.getNumberSamples() && !options.getGoogleVCF()) {
			throw new IllegalArgumentException(
					"To get the number of samples, you need to import mVCF file using Google APIs, and set --googleVCF=true");
		}
		
		// check whether user provided VCF tables IDs
		if (options.getVCFTables().isEmpty() && options.getInputVariant().isEmpty()
				&& options.getInputRegion().isEmpty()) {
			throw new IllegalArgumentException(
					"Please specify either a VCF/mVCF table (e.g., VCFTables=genomics-public-data:platinum_genomes.variants) \n"
							+ " OR a variant (e.g., chr13:68482097:68482098:CC:TT) \n"
							+ " OR a region (e.g., chr17:1000:2001) \n");
		}
		
		if (!options.getSearchRegions().isEmpty()) {
				checkRegions(options.getSearchRegions());

		}
			
		if (options.getBigQuerySort()) {
			// check whether user provided VCF tables IDs
			if (options.getLocalOutputFilePath().isEmpty()) {
				throw new IllegalArgumentException(
						"Please specify the path to the local output file \n");
			}	
		}
			
		try {
			long startTime = System.currentTimeMillis();
	
			String queryString = "";
			boolean LegacySql = false;
			if (!options.getVCFTables().isEmpty()) { // default empty
				if (options.getSampleId().isEmpty()) { // default empty
					if (options.getGeneBasedAnnotation()) { //default false
						if (options.getGeneBasedMinAnnotation()) {
							LOG.info("<============ Gene-based Annotation (mVCF) - Closest Genes (Min) ============>");
							queryString = BigQueryFunctions.prepareGeneBasedQueryConcatFields_mVCF_Range_Min_StandardSQL_RefGene(
									options.getVCFTables(), options.getVCFCanonicalizeRefNames(),
									options.getGenericAnnotationTables(), options.getGenericCanonicalizeRefNames(),
									options.getProximityThreshold(), options.getOnlyIntrogenic(),
									options.getCreateVCF(), options.getGeneBasedMinAnnotation(), 
									options.getSearchRegions());						
							
						} else {
							LOG.info("<============ Gene-based Annotation (mVCF) - Closest Genes (Range) ============>");
							queryString = BigQueryFunctions.prepareGeneBasedQueryConcatFields_mVCF_Range_Min_StandardSQL(
									options.getVCFTables(), options.getVCFCanonicalizeRefNames(),
									options.getGenericAnnotationTables(), options.getGenericCanonicalizeRefNames(),
									options.getProximityThreshold(), options.getOnlyIntrogenic(),
									options.getCreateVCF(), options.getGeneBasedMinAnnotation(), 
									options.getSearchRegions(), 
									options.getGoogleVCF());
						}
//						queryString = BigQueryFunctions.prepareGeneBasedQueryConcatFields_mVCF_Range_Min_StandardSQL(
//								options.getVCFTables(), options.getVCFCanonicalizeRefNames(),
//								options.getGenericAnnotationTables(), options.getGenericCanonicalizeRefNames(),
//								options.getProximityThreshold(), options.getOnlyIntrogenic(),
//								options.getCreateVCF(), options.getGeneBasedMinAnnotation(), 
//								options.getSearchRegions(), 
//								options.getGoogleVCF());
	
					} else {// Variant-based or Interval-based annotation
						LOG.info(
								"<============ Variant-based Annotation OR/AND Interval-based Annotation (mVCF) ============>");
						if (options.getSearchRegions().isEmpty()) { // default empty
							if (options.getCreateVCF()) {
								
								queryString = BigQueryFunctions.prepareAnnotateVariantQueryConcatFields_mVCF(options.getVCFTables(),
									options.getVCFCanonicalizeRefNames(), options.getGenericAnnotationTables(), options.getUCSCGenericAnnotationTables(),
									options.getGenericCanonicalizeRefNames(), options.getVariantAnnotationTables(),
									options.getVariantAnnotationCanonicalizeRefNames(), options.getCreateVCF(), 
									false, options.getGoogleVCF(),
									options.getNumberSamples());
								LegacySql = true;

							}else {
								
								//Annotate the input VCF file using UCSC_knowGene in case we have any generic or UCSCGeneric tables  
								if (options.getUCSCGenericAnnotationTables()!=null || options.getGenericAnnotationTables()!=null) {
									String UCSC_KnownGene="";
									if (options.getBuild().equalsIgnoreCase("hg19"))
										UCSC_KnownGene= "gbsc-gcp-project-cba:AnnotationHive_Public.hg19_UCSC_knownGene:name";
									else if(options.getBuild().equalsIgnoreCase("hg38"))
										UCSC_KnownGene= "gbsc-gcp-project-cba:AnnotationHive_Public.hg38_UCSC_knownGene:name";
									
									
									//Convert Google VCF table to AnnotationHive's VCF version
									if(options.getGoogleVCF())
									{
										String Google_VCF= options.getVCFTables();
										
										queryString = "SELECT REPLACE(reference_name, '', '') as reference_name, start_position as start, "
												+ "end_position as `END`, reference_bases, alternate_bases.alt as alternate_bases  FROM `"
												+ Google_VCF.split(":")[0] + "." + Google_VCF.split(":")[1]  +"`, unnest (alternate_bases) as alternate_bases ";

										options.setVCFTables(options.getProject() + ":" +options.getBigQueryDatasetId() + "." + Google_VCF.split(":")[1].split("\\.")[1] + "_AnnotationHiveVCF");
										
										LOG.info("STEP 0:" + queryString);

										runQuery(queryString, options.getBigQueryDatasetId(), 
												Google_VCF.split(":")[1].split("\\.")[1] + "_AnnotationHiveVCF" , true,
												options.getMaximumBillingTier(), false, false, false);
										
										options.setGoogleVCF(false);
									}
									
									queryString=BigQueryFunctions.prepareAnnotateVariantQueryConcatFields_mVCF_StandardSQL_Concat(
											options.getVCFTables(),options.getVCFTables(),
											options.getVCFCanonicalizeRefNames(), null, UCSC_KnownGene,
											options.getGenericCanonicalizeRefNames(), null,
											options.getVariantAnnotationCanonicalizeRefNames(), options.getCreateVCF(), 
											false, options.getGoogleVCF(),
											options.getNumberSamples(), options.getBuild(),true, options.getBinSize());

									LOG.info("STEP 1:" + queryString);
									
									runQuery(queryString, options.getBigQueryDatasetId(), 
									options.getOutputBigQueryTable() +"_Join_wUCSC", true,
											options.getMaximumBillingTier(), false, false, false);
		//VCF -> alternate   
									queryString = BigQueryFunctions.prepareAnnotateVariantQueryConcatFields_mVCF_StandardSQL_Concat(options.getVCFTables(),
											options.getBigQueryDatasetId() + ":" + options.getOutputBigQueryTable() +"_Join_wUCSC",
											options.getVCFCanonicalizeRefNames(), options.getUCSCGenericAnnotationTables(), 
											options.getGenericAnnotationTables(),
											options.getGenericCanonicalizeRefNames(), options.getVariantAnnotationTables(),
											options.getVariantAnnotationCanonicalizeRefNames(), false, 
											false, options.getGoogleVCF(), false, options.getBuild(), false, options.getBinSize());
								}
								else {
									queryString = BigQueryFunctions.prepareAnnotateVariantQueryConcatFields_mVCF_StandardSQL_Concat(options.getVCFTables(), "",
										options.getVCFCanonicalizeRefNames(), options.getUCSCGenericAnnotationTables(), options.getGenericAnnotationTables(),
										options.getGenericCanonicalizeRefNames(), options.getVariantAnnotationTables(),
										options.getVariantAnnotationCanonicalizeRefNames(), options.getCreateVCF(), 
										false, options.getGoogleVCF(),
										options.getNumberSamples(), options.getBuild(), false, options.getBinSize());
								}
							}
						}else { //In case we ONLY have variant annotations
							
							queryString = BigQueryFunctions.prepareAnnotateVariantQueryRegion_Name(options.getVCFTables(),
								options.getVCFCanonicalizeRefNames(), options.getGenericAnnotationTables(),
								options.getGenericCanonicalizeRefNames(), options.getVariantAnnotationTables(),
								options.getVariantAnnotationCanonicalizeRefNames(), "", true, false, options.getSearchRegions(), options.getGoogleVCF());
							
							//TODO:change it back
	//						queryString = BigQueryFunctions.prepareAnnotateVariantQueryConcatFields_mVCF_GroupBy(
	//								options.getVCFTables(),
	//								options.getVCFCanonicalizeRefNames(), options.getGenericAnnotationTables(),
	//								options.getGenericCanonicalizeRefNames(), options.getVariantAnnotationTables(),
	//								options.getVariantAnnotationCanonicalizeRefNames(), false, true, options.getSearchRegions());
							LegacySql = true;

						}
							
					}
				} else { // e.g., LP6005038-DNA_H11
					if (options.getGeneBasedAnnotation()) {
						if (options.getGeneBasedMinAnnotation()) {
							LOG.info(
									"<============ Gene-based Annotation (VCF - One Sample) - Closest Genes (Min) ============>");
						} else {
							LOG.info(
									"<============ Gene-based Annotation (VCF - One Sample) - Closest Genes (Range) ============>");
						}
						queryString = BigQueryFunctions.prepareGeneBasedQueryConcatFields_Range_Min_StandardSQL(
								options.getVCFTables(), options.getVCFCanonicalizeRefNames(),
								options.getGenericAnnotationTables(), options.getGenericCanonicalizeRefNames(),
								options.getSampleId(), options.getProximityThreshold(), options.getOnlyIntrogenic(),
								options.getCreateVCF(), options.getGeneBasedMinAnnotation(), options.getSearchRegions(), options.getGoogleVCF());
					} else {
						LOG.info(
								"<============ Variant-based Annotation OR/AND Interval-based Annotation (VCF - One Sample) ============>");
						queryString = BigQueryFunctions.prepareAnnotateVariantQueryWithSampleNames(options.getVCFTables(),
								options.getVCFCanonicalizeRefNames(), options.getGenericAnnotationTables(),
								options.getGenericCanonicalizeRefNames(), options.getVariantAnnotationTables(),
								options.getVariantAnnotationCanonicalizeRefNames(), options.getSampleId(),
								options.getCreateVCF(), false, options.getGoogleVCF());
						LegacySql = true;
					}
				}
			} else if (!options.getInputVariant().isEmpty()) {
				LOG.info("<============ Variant-based Annotation ( " + options.getInputVariant() + " ) ============>");
	
				// QC
				String[] VAs = options.getInputVariant().split(",");
				List<String[]> listVA = new ArrayList<String[]>(VAs.length);
				
				for (String v : VAs)
					listVA.add(QC_Test_Input_Variant(v, false));
	
				if (listVA.isEmpty()) {
					throw new IllegalArgumentException("Please specify a variant (e.g., chr13:68482097:68482098:CC:TT) \n");
				}
	
				if (listVA.size() == 1) {
					queryString = BigQueryFunctions.prepareOneVariantQuery_StandardSQL(listVA.get(0),
							options.getVariantAnnotationTables(), options.getVariantAnnotationCanonicalizeRefNames());
				} else {// >1
						// create a small VCF table w/ the input dataset
					String tempTableName = "`" + options.getProjectId() + "." + options.getBigQueryDatasetId() + "."
							+ "AnnotationHiveTempVCFTable`";
					queryString = BigQueryFunctions.createTempVCFTable(listVA, tempTableName, true);
					// Create the temp VCF table
					runQuery(queryString, options.getBigQueryDatasetId(), "AnnotationHiveTempVCFTable", true,
							options.getMaximumBillingTier(), LegacySql, false, false);
	
					String tempTableNameLegacy = options.getProjectId() + ":" + options.getBigQueryDatasetId() + "."
							+ "AnnotationHiveTempVCFTable";
	
					queryString = BigQueryFunctions.prepareAnnotateVariantQueryWithSampleNames(tempTableNameLegacy,
							options.getVCFCanonicalizeRefNames(), options.getGenericAnnotationTables(),
							options.getGenericCanonicalizeRefNames(), options.getVariantAnnotationTables(),
							options.getVariantAnnotationCanonicalizeRefNames(), "", false, true, options.getGoogleVCF());
				}
	
				LegacySql = true;
	
			} else { // options.getInputRegion().isEmpty()
	
				LOG.info("<============ Region-based Annotation ( " + options.getInputVariant() + " ) ============>");
	
				if (options.getVariantAnnotationTables() != null) {
					throw new IllegalArgumentException(
							"Region-based annotation (the input regions) only accepts generic annotation!\n");
				}
	
				// QC
				String[] Regions = options.getInputRegion().split(",");
				List<String[]> listRegions = new ArrayList<String[]>(Regions.length);

				for (String v : Regions)
					listRegions.add(QC_Test_Input_Variant(v, true));
	
				if (listRegions.isEmpty()) {
					throw new IllegalArgumentException("Please specify a region (e.g., chr17:1000:2001) \n");
				}
	
				if (listRegions.size() == 1) {
					queryString = BigQueryFunctions.prepareOneRegionQuery_StandardSQL(listRegions.get(0),
							options.getGenericAnnotationTables(), options.getGenericCanonicalizeRefNames());
				} else {// >1
					// create a small VCF table w/ the input dataset
					String tempTableName = "`" + options.getProjectId() + "." + options.getBigQueryDatasetId() + "."
							+ "AnnotationHiveTempVCFTable`";
					queryString = BigQueryFunctions.createTempVCFTable(listRegions, tempTableName, false);
					// Create the temp VCF table
					runQuery(queryString, options.getBigQueryDatasetId(), "AnnotationHiveTempVCFTable", true,
							options.getMaximumBillingTier(), LegacySql, false, false);
	
					String tempTableNameLegacy = options.getProjectId() + ":" + options.getBigQueryDatasetId() + "."
							+ "AnnotationHiveTempVCFTable";
	
					queryString = BigQueryFunctions.prepareAnnotateVariantQueryWithSampleNames(tempTableNameLegacy,
							options.getVCFCanonicalizeRefNames(), options.getGenericAnnotationTables(),
							options.getGenericCanonicalizeRefNames(), options.getVariantAnnotationTables(),
							options.getVariantAnnotationCanonicalizeRefNames(), "", false, true, options.getGoogleVCF());
				}
				LegacySql = true;
	
			}
	
			LOG.info("Query: " + queryString);
	
			////////////////////////////////// STEP1/////////////////////////////////////
			////////////////////////////////// Joins/////////////////////////////////////
			if(!options.getTableExists()) 
			{
				runQuery(queryString, options.getBigQueryDatasetId(), options.getOutputBigQueryTable()+"_temp", true,
					options.getMaximumBillingTier(), LegacySql, false, false);
				
				//Partition By
				String PartitionQuery = "CREATE TABLE `"
						+ options.getProjectId() + "." + options.getBigQueryDatasetId() + "." +  options.getOutputBigQueryTable() // "gbsc-gcp-project-cba.Clustering.VA_Crossmap_hg19_Cancer_10_WGS_With_SampleCount"
						+ "` PARTITION BY partition_date_please_ignore "
						+ " CLUSTER BY reference_name, start , `end` AS ("
						+ " SELECT *, DATE('1980-01-01') partition_date_please_ignore FROM "
						+ " `"
						+ options.getProjectId() + "." + options.getBigQueryDatasetId() + "." + options.getOutputBigQueryTable()+"_temp"
						+ "`)";
				
				LOG.info("Partition Query: " + PartitionQuery);

				//Create a partitioned table 
				runQuery(PartitionQuery, options.getBigQueryDatasetId(), options.getOutputBigQueryTable(), true,
						options.getMaximumBillingTier(), false, false, true);
				
				//Remove the temp table
				BigQueryFunctions.deleteTable(options.getBigQueryDatasetId(), options.getOutputBigQueryTable()+"_temp");

				
			}
			long tempEstimatedTime = System.currentTimeMillis() - startTime;
			LOG.info("Execution Time for Join Query: " + tempEstimatedTime);
	
			// Remove the temporarily VCF file created based on the input list of
			// variants/regions
			if (!options.getInputRegion().isEmpty() || !options.getInputVariant().isEmpty()) {
				BigQueryFunctions.deleteTable(options.getBigQueryDatasetId(), "AnnotationHiveTempVCFTable");
			}
	
			if (options.getCreateVCF()) {
	
				////////////////////////////////// STEP2////////////////////////////////////
				////////////////////////////////// Sort/////////////////////////////////////
				startTime = System.currentTimeMillis();
				runSort();
				tempEstimatedTime = System.currentTimeMillis() - startTime;
				LOG.info("Execution Time for Sort Query: " + tempEstimatedTime);
	
				///////////////// STEP3: Delete the Intermediate
				///////////////// Table///////////////////////////
				// TODO: Add a new condition here
				if(options.getDeleteOutputTable())
					BigQueryFunctions.deleteTable(options.getBigQueryDatasetId(), options.getOutputBigQueryTable());
			}
			
		}catch (Exception e) {			
			LOG.severe(e.getMessage());
		}

		
	}
	
	/**
	 * <h1>This function is checks the input searchRegions
	 */
	private static void checkRegions(String searchRegions) {
		String[] regions = searchRegions.split(",");
		for (String rs : regions ) {
			String [] temp = rs.split(":");
			if(temp.length!=3 || (Integer.parseInt(temp[1])>Integer.parseInt(temp[2])))
			{
				throw new IllegalArgumentException("Input Search Regions is not correct (e.g., chr13:32889610:32973808,chr17:41196311:41277499)");
			}
		}
	}

	private static String[] QC_Test_Input_Variant(String inputVariant, boolean region_based) {

		try {
			String[] va = inputVariant.split(":");

			int numParameters = 5; // Variant
			if (region_based)
				numParameters = 3; // region

			if (va.length < numParameters || va.length > numParameters) {
				if (!region_based)
					throw new IllegalArgumentException("The input variant is not correct. Here are three examples: \n"
							+ "SNP example: \"chr17:1001:1001:A:C\" \n"
							+ "Insertion examples: \"chr17:1001:1001:A:ACTT\" OR  \"chr17:1001:1001:-:CTT\"  \n"
							+ "Deletion examples: \"chr17:1001:1003:ATT:-\" \n");
				else
					throw new IllegalArgumentException(
							"The input region is not correct. Here is an example: \n" + "\"chr17:1001:2001\" \n");
			}

			String chr = "";
			if (va[0].contains("chr")) {
				chr = va[0].replace("chr", "");
			}
			if (!chr.contentEquals("M") && !chr.contentEquals("MT") && !chr.contentEquals("X")
					&& !chr.contentEquals("Y")) {
				int chrID = Integer.parseInt(chr);
				if (chrID < 1 && chrID > 23) {
					if (!region_based)
						throw new IllegalArgumentException(
								"The input chormosome is not correct. Here are three examples: \n"
										+ "SNP example: \"chr17:1001:1001:A:C\" \n"
										+ "Insertion examples: \"chr17:1001:1001:A:ACTT\" OR  \"chr17:1001:1001:-:CTT\"  \n"
										+ "Deletion examples: \"chr17:1001:1003:ATT:-\" \n");
					else
						throw new IllegalArgumentException(
								"The input region is not correct. Here is an example: \n" + "\"chr17:1001:2001\" \n");
				}
			}
			va[0] = chr;

			int start = Integer.parseInt(va[1]);
			int end = Integer.parseInt(va[2]);
			if (start < 0 || end < 0 || end < start) {
				if (!region_based)
					throw new IllegalArgumentException(
							"The input chormosome is not correct. Here are three examples: \n"
									+ "SNP example: \"chr17:1001:1001:A:C\" \n"
									+ "Insertion examples: \"chr17:1001:1001:A:ACTT\" OR  \"chr17:1001:1001:-:CTT\"  \n"
									+ "Deletion examples: \"chr17:1001:1003:ATT:-\" \n");
				else
					throw new IllegalArgumentException(
							"The input region is not correct. Here is an example: \n" + "\"chr17:1001:2001\" \n");
			}

			// TODO: check input reference bases and alternate bases

			// if (va[3].matches("^[a-A-T-t-c-C-T-t-g-G]+$")) {
			// // contains only listed chars
			// } else {
			// // contains other chars
			// }

			return va;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	private static void runQuery(String queryString, String bigQueryDatasetId, String outputBigQueryTable,
			boolean allowLargeResults, int maximumBillingTier, boolean LegacySql, boolean Update, boolean DDL) {

		try {
			BigQueryFunctions.runQueryPermanentTable(queryString, bigQueryDatasetId, outputBigQueryTable,
					allowLargeResults, maximumBillingTier, LegacySql, Update, DDL);
			
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * <h1>This function sorts the output of BigQuery
	 * 
	 * @return
	 */

	private static void runSort() {

		if (!options.getBigQuerySort()) {
			
			
			////////////////////////////////// STEP2: Run Dataflow
			////////////////////////////////// Sort/////////////////////////////////////
			PCollection<TableRow> variantData = p
					.apply(BigQueryIO.read().fromQuery("SELECT  * FROM [" + options.getProjectId() + ":"
							+ options.getBigQueryDatasetId() + "." + options.getOutputBigQueryTable() + "]"));

			PCollection<KV<Long, TableRow>> variantDataKV = variantData.apply(ParDo.of(new BinVariantsFn()));
			PCollection<KV<Integer, KV<Long, Iterable<String>>>> SortedBin;
			if (!options.getGeneBasedAnnotation()) {
				// GroupBy each Bin and then sort it
				SortedBin = variantDataKV.apply("Group By Variants Based on BinID", GroupByKey.<Long, TableRow>create())

						//////////////////////////
						// SortedBin.
						//////////////////////////

						// Sort bins in parallel
						.apply("Sort Variants inside Bins", ParDo
								.of(new DoFn<KV<Long, Iterable<TableRow>>, KV<Integer, KV<Long, Iterable<String>>>>() {

									private static final long serialVersionUID = 803837233177187278L;

									@ProcessElement
									public void processElement(ProcessContext c) {

										KV<Long, Iterable<TableRow>> e = c.element();
										Long key = e.getKey();

										List<TableRow> records = Lists.newArrayList(e.getValue()); // Get a modifiable
																									// list.
										Collections.sort(records, VARIANT_SEGMENT_COMPARATOR);
										List<String> mergedItems = Lists.newArrayList(); // recordsString;
										String buffer = "";
										TableRow oldRow = null;
										
										//Do we have five columns before annotation fileds to 6?
										int fixedColumns=5; // chrm, start, end, ref, alt
										
										for (TableRow row : records) {
											if (row.get("num_samples")!=null)
												fixedColumns=6; // chrm, start, end, ref, alt, number of samples
											
											String temp = ""; // for every row, temp would be empty
											Object[] fieldValues = row.values().toArray();
											for (int index = 0; index < fieldValues.length; index++) {
												if (fieldValues[index] != null) {
													if (index == 1) // Start [Change
																	// 0-base to
																	// 1-base]
													{
														long tempStart = Long.parseLong(fieldValues[index].toString())
																+ 1; //convert to 1-based
														temp += tempStart + "\t";
													} else {
														if (index == fixedColumns) // chrm, start, end, ref, alt => first 5 columns
															temp += "~"; // a special character b/w annotations for the
																			// same position
														temp += fieldValues[index].toString() + "\t";
													}
												}
											}
											if (oldRow == null) // if this is the first row
												buffer = temp;
											else if (oldRow != null) {
												if (row.get("start").toString().equals(oldRow.get("start").toString())
														&& row.get("end").toString()
																.equals(oldRow.get("end").toString())
														&& row.get("alternate_bases").toString()
																.equals(oldRow.get("alternate_bases").toString())) {

													for (int index = fixedColumns; index < fieldValues.length; index++) {
														if (fieldValues[index] != null) {
															// add ";" before other
															// annotations
															if (index == fixedColumns)
																buffer += "~" + fieldValues[index].toString() + "\t";
															else
																buffer += fieldValues[index].toString() + "\t";
														}
													}
												} else {
													LOG.warning("buffer: " + buffer);
													mergedItems.add(sortMatched(buffer));
													buffer = temp;
												}
											}

											oldRow = row;
										}
										mergedItems.add(sortMatched(buffer));

										Iterable<String> sortedBin = mergedItems; // recordsString;
										if (mergedItems.size() > 0) {
											// 9 Digits Padding
											int chrm = (int) (key / 1000000000);
											c.output(KV.of(chrm, KV.of(key, sortedBin)));
										}
									}

									// This function sort annotations for each position -> Annotation1, Annotation2,
									// .., AnnotationN
									private String sortMatched(String buffer) {

										String ret = "";
										HashMap<Integer, String> hmap = new HashMap<Integer, String>();
										String[] listString = buffer.split("~");
										// The first item in listString is "ChromID,
										// Start, End, Ref, Alt"

										ret += listString[0];
										LOG.warning("ret += listString[0]: " + ret);

										// Start from the second item
										for (int index = 1; index < listString.length; index++) {
											// The first item of annotation has a
											// unique integer id following ':'
											String[] Annotation = listString[index].split(":");
											LOG.warning("listString [" + index + "] = " + listString[index]);
											if (Annotation != null && Annotation.length == 2) {
												LOG.warning("Annotation [0] = " + Annotation[0]);
												LOG.warning("Annotation [1] = " + Annotation[1]);
												// in case of overlap
												if (hmap.containsKey(Integer.parseInt(Annotation[0]))) {
													hmap.put((index + listString.length), Annotation[1]); 
													// this case is when we have
													// multiple annotation from the same annotation dataset then put
													// them at the end.
												} else {
													hmap.put(Integer.parseInt(Annotation[0]), Annotation[1]);
												}
											} else {
												if (Annotation.length > 2) {
													// This is the case when
													// annotation contains ":"
													String temp = "";
													for (index = 1; index < Annotation.length; index++)
														temp += Annotation[index];
													hmap.put(Integer.parseInt(Annotation[0]), temp);
												} else
													LOG.severe("Erroneous Buffer  = " + buffer);
											}
										}
										// Sort the keys
										SortedSet<Integer> keys = new TreeSet<Integer>(hmap.keySet());

										// merge sorted values
										for (final Iterator<Integer> it = keys.iterator(); it.hasNext();) {
											int annId = it.next();
											ret += Integer.toString(annId) + ":" + hmap.get(annId);
										}

										return ret;
									}

								}));
			} else { // Gene-Based Annotation Multiple Matches from the same dataset handling
						// GroupBy each Bin and then sort it
				SortedBin = variantDataKV.apply("Group By Variants Based on BinID", GroupByKey.<Long, TableRow>create())

						//////////////////////////
						// SortedBin.
						//////////////////////////

						// Sort bins in parallel
						.apply("Sort Variants inside Bins", ParDo
								.of(new DoFn<KV<Long, Iterable<TableRow>>, KV<Integer, KV<Long, Iterable<String>>>>() {

									private static final long serialVersionUID = 803837233177187278L;

									@ProcessElement
									public void processElement(ProcessContext c) {

										KV<Long, Iterable<TableRow>> e = c.element();
										Long key = e.getKey();
										List<TableRow> records = Lists.newArrayList(e.getValue());
										Collections.sort(records, VARIANT_SEGMENT_COMPARATOR);
										List<String> mergedItems = Lists.newArrayList();
										String buffer = "";

										TableRow oldRow = null;
										for (TableRow row : records) {
											String temp = ""; // for every row, temp
																// would be empty
											Object[] fieldValues = row.values().toArray();
											for (int index = 0; index < fieldValues.length; index++) {
												if (fieldValues[index] != null) {
													if (index == 1) // Start [Change
																	// 0-base to
																	// 1-base]
													{
														long tempStart = Long.parseLong(fieldValues[index].toString())
																+ 1;
														temp += tempStart + "\t";
													} else {
														if (index == 5) // chrm, start, end, ref, alt => first 5 columns
															temp += "~"; // a special character b/w annotations for the
																			// same position
														temp += fieldValues[index].toString() + "\t";
													}
												}
											}
											if (oldRow == null) // if this is the
																// first row
												buffer = temp;
											else if (oldRow != null) {
												if (row.get("start").toString().equals(oldRow.get("start").toString())
														&& row.get("end").toString()
																.equals(oldRow.get("end").toString())
														&& row.get("alternate_bases").toString()
																.equals(oldRow.get("alternate_bases").toString())) {

													for (int index = 5; index < fieldValues.length; index++) {
														if (fieldValues[index] != null) {
															// add ";" before other
															// annotations
															if (index == 5)
																buffer += "~" + fieldValues[index].toString() + "\t";
															else
																buffer += fieldValues[index].toString() + "\t";

														}
													}
												} else {
													LOG.warning("buffer: " + buffer);
													mergedItems.add(sortMatched(buffer));
													buffer = temp;
												}
											}

											oldRow = row;
										}
										mergedItems.add(sortMatched(buffer));

										Iterable<String> sortedBin = mergedItems; // recordsString;
										if (mergedItems.size() > 0) {
											int chrm = (int) (key / 1000000000); // 9 digits padding
											c.output(KV.of(chrm, KV.of(key, sortedBin)));
										}
									}

									// This function sort annotations for each position -> Annotation1, Annotation2,
									// .., AnnotationN
									private String sortMatched(String buffer) {

										String ret = "";
										HashMap<Integer, String> hmap = new HashMap<Integer, String>();
										String[] listString = buffer.split("~");
										// The first item in listString is "ChromID,
										// Start, End, Ref, Alt"

										ret += listString[0];
										LOG.warning("ret += listString[0]: " + ret);

										// Start from the second item
										for (int index = 1; index < listString.length; index++) {
											// The first item of annotation has a
											// unique integer id following ':'
											String[] Annotation = listString[index].split(":");
											LOG.warning("listString [" + index + "] = " + listString[index]);
											if (Annotation != null && Annotation.length == 2) {
												LOG.warning("Annotation [0] = " + Annotation[0]);
												LOG.warning("Annotation [1] = " + Annotation[1]);
												// in case of overlap
												if (hmap.containsKey(Integer.parseInt(Annotation[0]))) {
													hmap.put(index, Annotation[1]); // increment + there is only one
																					// annotation dataset
												} else {
													hmap.put(Integer.parseInt(Annotation[0]), Annotation[1]);
												}
											} else {
												if (Annotation.length > 2) {
													// This is the case when
													// annotation contains ":"
													String temp = "";
													for (index = 1; index < Annotation.length; index++)
														temp += Annotation[index];
													hmap.put(Integer.parseInt(Annotation[0]), temp);
												} else
													LOG.severe("Erroneous Buffer  = " + buffer);
											}
										}
										// Sort the keys
										SortedSet<Integer> keys = new TreeSet<Integer>(hmap.keySet());

										// merge sorted values
										for (final Iterator<Integer> it = keys.iterator(); it.hasNext();) {
											int annId = it.next();
											ret += hmap.get(annId);
										}

										return ret;
									}

								}));
			}

			PCollection<KV<Integer, KV<Integer, Iterable<KV<Long, Iterable<String>>>>>> SortedChrm = SortedBin
					.apply("Group By BinID", GroupByKey.<Integer, KV<Long, Iterable<String>>>create())
					.apply("Sort By BinID ", ParDo.of(
							new DoFn<KV<Integer, Iterable<KV<Long, Iterable<String>>>>, KV<Integer, KV<Integer, Iterable<KV<Long, Iterable<String>>>>>>() {

								private static final long serialVersionUID = -8017322538250102739L;

								@org.apache.beam.sdk.transforms.DoFn.ProcessElement
								public void processElement(ProcessContext c) {

									KV<Integer, Iterable<KV<Long, Iterable<String>>>> e = c.element();
									Integer Secondary_key = e.getKey();

									ArrayList<KV<Long, Iterable<String>>> records = Lists.newArrayList(e.getValue()); // Get
																														// a
																														// modifiable
																														// list.
									LOG.warning("Total Mem: " + Runtime.getRuntime().totalMemory());
									LOG.warning("Free Mem: " + Runtime.getRuntime().freeMemory());

									Collections.sort(records, BinID_COMPARATOR);
									if (records.size() > 0) {
										Integer Primary_key = 1;
										LOG.warning("Primary_key: " + Primary_key);
										LOG.warning("Secondary_key: " + Secondary_key);
										c.output(KV.of(Primary_key,
												KV.of(Secondary_key, (Iterable<KV<Long, Iterable<String>>>) records)));
									}
								}
							}));

			SortedChrm
					.apply("Group By Chromosome",
							GroupByKey.<Integer, KV<Integer, Iterable<KV<Long, Iterable<String>>>>>create())
					.apply("Sort By Chromosome", ParDo.of(
							new DoFn<KV<Integer, Iterable<KV<Integer, Iterable<KV<Long, Iterable<String>>>>>>, String>() {
								private static final long serialVersionUID = 403305704191115836L;

								@org.apache.beam.sdk.transforms.DoFn.ProcessElement
								public void processElement(ProcessContext c) {

									ArrayList<KV<Integer, Iterable<KV<Long, Iterable<String>>>>> records = Lists
											.newArrayList(c.element().getValue()); // Get
																					// a
																					// modifiable
																					// list.
									Collections.sort(records, ChrmID_COMPARATOR);

									for (KV<Integer, Iterable<KV<Long, Iterable<String>>>> ChromLevel : records) {
										for (KV<Long, Iterable<String>> BinLevel : ChromLevel.getValue()) {

											for (String ItemLevel : BinLevel.getValue()) {
												/////////////////////////
												c.output(ItemLevel);
											}
										}
									}

								}
							}))
					.apply("VCF", TextIO.write().to(options.getBucketAddrAnnotatedVCF()));

			p.run().waitUntilFinish();


			
			Path VCF_Filename = Paths.get(options.getBucketAddrAnnotatedVCF());
			LOG.info("");
			LOG.info("");
			LOG.info("------------------------------------------------------------------------\n"
					+ "Header: \n\n " + createHeader() + "\n\n"
					+ "INFO: To download the annotated VCF file from Google Cloud, run the following command:\n"
					+ "INFO: \t ~: gsutil cat " + options.getBucketAddrAnnotatedVCF() + "* > "
					+ VCF_Filename.getFileName().toString() + "\n" + "\n"
					+ "INFO: To remove the output files from the cloud storage run the following command:\n"
					+ "INFO: \t ~: gsutil rm " + options.getBucketAddrAnnotatedVCF() + "* \n"
					+ "INFO: ------------------------------------------------------------------------ \n\n");
		} else {

			////////////////////////////////// STEP2: Sort Per chromosome in BigQuery ->
			////////////////////////////////// Output is a local
			////////////////////////////////// file/////////////////////////////////////
			try {
				
				if (!options.getTableExists()) {
					String queryStat = "UPDATE   " + "`" + options.getProject() + "."
							+ options.getBigQueryDatasetId() + "." + options.getOutputBigQueryTable() + "` "
							+ " SET start = start + 1 WHERE chrm <>\"\" ";
					LOG.warning(queryStat);
					 runQuery(queryStat, options.getBigQueryDatasetId(), options.getOutputBigQueryTable(), true,
							options.getMaximumBillingTier(), false, true, false);				
				}
				BigQueryFunctions.sortByBin(options.getProject(), options.getBigQueryDatasetId(),
						options.getOutputBigQueryTable(), options.getLocalOutputFilePath(), options.getBinSize());
				
				LOG.info("");
				LOG.info("");
				LOG.info("------------------------------------------------------------------------\n"
						+ "INFO: Header: \n\n " + createHeader() + "\n\n"
						+ "INFO: File: " + options.getLocalOutputFilePath()
						+ "INFO: ------------------------------------------------------------------------ \n\n");
				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	
	private static String createHeader() {
		String Header = "Chrom\tStart\tEnd\tRef\tAlt\t";
		if (options.getNumberSamples())
		{
			Header += "Num. of Samples\t";
		}
		int numbVariants=0;
		if (options.getVariantAnnotationTables() != null) {
			String[] VariantAnnotationTables = options.getVariantAnnotationTables().split(",");
			numbVariants= VariantAnnotationTables.length;
			for (int index = 0; index < VariantAnnotationTables.length; index++) {

				/*
				 * Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF
				 * :GME_NWA:GME_NEA ProjectId: gbsc-gcp-project-cba DatasetId:
				 * PublicAnnotationSets TableId: hg19_GME Features: GME_AF:GME_NWA:GME_NEA
				 */

				String[] TableInfo = VariantAnnotationTables[index].split(":");
				Header += "<" + TableInfo[1].split("\\.")[1]+"(";
				
				for (int innerIndex=2; innerIndex<TableInfo.length; innerIndex++) {
					if(innerIndex+1<TableInfo.length)
						Header += TableInfo[innerIndex] + "/";
					else
						Header += TableInfo[innerIndex]+")";

				}
							
				Header += "," + (index + 1) + ">";
				if (index + 1 < VariantAnnotationTables.length)
					Header += "\t";
			}
		}

		if (options.getGenericAnnotationTables() != null) {
			if(numbVariants>0)
				Header += "\t";
			String[] TranscriptAnnotationTables = options.getGenericAnnotationTables().split(",");
			for (int index = 0; index < TranscriptAnnotationTables.length; index++) {

				String[] TableInfo = TranscriptAnnotationTables[index].split(":");
//				Header += "<" + TableInfo[1].split("\\.")[1] +  TableInfo[2] + "," + (index + 1 + numbVariants) + ">";
//				if (index + 1 < TranscriptAnnotationTables.length)
//					Header += "\t";
				Header += "<" + TableInfo[1].split("\\.")[1]+"(";
				
				for (int innerIndex=2; innerIndex<TableInfo.length; innerIndex++) {
					if(innerIndex+1<TableInfo.length)
						Header += TableInfo[innerIndex] + "/";
					else
						Header += TableInfo[innerIndex]+")";

				}
							
				Header += "," + (index + 1) + ">";
				if (index + 1 < TranscriptAnnotationTables.length)
					Header += "\t";
			}
		}
		
		return Header;
	}

	static final class BinVariantsFn extends DoFn<TableRow, KV<Long, TableRow>> {
		/**
		 * This function assign a unique ID to each variant chrId+start => 9 digits It
		 * take cares of the padding as well. It assigns 23, 24, and 24 to chromosome X,
		 * Y and M correspondingly.
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
			Options options = context.getPipelineOptions().as(Options.class);
			int binSize = options.getBinSize();
			TableRow rowVariant = context.element();
			long startBin = getStartBin(binSize, rowVariant);

			String key = rowVariant.get("chrm").toString();
			if (key.equalsIgnoreCase("Y"))
				key = "23";
			else if (key.equalsIgnoreCase("X"))
				key = "24";
			else if (key.equalsIgnoreCase("M") || key.equalsIgnoreCase("MT"))
				key = "25";

			String BinNum = Long.toString(startBin);
			int numZeros = 9 - BinNum.length();

			// Padding
			for (int index = 1; index <= numZeros; index++) {
				key += "0"; // Shift ChrmID
			}
			key += BinNum;

			context.output(KV.of(Long.parseLong(key), rowVariant));

		}
	}

	private static final Ordering<TableRow> BY_START = Ordering.natural().onResultOf(new Function<TableRow, Long>() {
		@Override
		public Long apply(TableRow variant) {
			return Long.parseLong(variant.get("start").toString());
		}
	});

	private static final Ordering<TableRow> BY_FIRST_OF_ALTERNATE_BASES = Ordering.natural().nullsFirst()
			.onResultOf(new Function<TableRow, String>() {
				@Override
				public String apply(TableRow variant) {
					if (null == variant.get("alternate_bases").toString()
							|| variant.get("alternate_bases").toString().isEmpty()) {
						return null;
					}
					return variant.get("alternate_bases").toString();
				}
			});

	static final Comparator<TableRow> VARIANT_SEGMENT_COMPARATOR = BY_START.compound(BY_FIRST_OF_ALTERNATE_BASES);

	private static final Ordering<KV<Long, Iterable<String>>> BY_BinID = Ordering.natural()
			.onResultOf(new Function<KV<Long, Iterable<String>>, Long>() {
				@Override
				public Long apply(KV<Long, Iterable<String>> variant) {
					return Long.parseLong(variant.getKey().toString());
				}
			});

	static final Comparator<KV<Long, Iterable<String>>> BinID_COMPARATOR = BY_BinID;

	private static final Ordering<KV<Integer, Iterable<KV<Long, Iterable<String>>>>> BY_ChrmID = Ordering.natural()
			.onResultOf(new Function<KV<Integer, Iterable<KV<Long, Iterable<String>>>>, Integer>() {
				@Override
				public Integer apply(KV<Integer, Iterable<KV<Long, Iterable<String>>>> variant) {
					return Integer.parseInt(variant.getKey().toString());
				}
			});

	static final Comparator<KV<Integer, Iterable<KV<Long, Iterable<String>>>>> ChrmID_COMPARATOR = BY_ChrmID;

}
