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

import java.io.IOException;
import java.security.GeneralSecurityException;
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
import org.apache.beam.sdk.extensions.sorter.*;

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
 * @param bigQueryDataset
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
 * @version 1.0
 * @since 2018-02-01
 */

public final class BigQueryAnnotateVariants {

	public static interface Options extends GenomicsOptions {

		@Description("This provides BigQuery Dataset ID.")
		@Default.String("")
		String getBigQueryDatasetId();

		void setBigQueryDataset(String BigQueryDataset);

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

		@Description("This provides the address of Variant Annotation tables.")
		String getVariantAnnotationTables();

		void setVariantAnnotationTables(String VariantAnnotationTables);

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

		@Description("User can specifiy the format of output to be a table (true/false - default is false (txt) )")
		@Default.Boolean(false)
		Boolean getOutputFormatTable();

		void setOutputFormatTable(Boolean outputFormatTable);

	}

	private static Options options;
	private static Pipeline p;
	private static final Logger LOG = Logger.getLogger(BigQueryAnnotateVariants.class.getName());

	/**
	 * <h1>This function is the main function that populates queries and calls
	 * dataflow/BigQuering sorting
	 */
	public static void run(String[] args) throws GeneralSecurityException, IOException {

		// TODO: Make sure all VCF files and annotation sets are in the same
		// systems (e.g., hg19/GRCh37 or hg20/GRCh38)

		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		// Here is the dataflow pipeline
		p = Pipeline.create(options);

		// check whether user provided any generic or variant annotation table IDs
		if (options.getGenericAnnotationTables() == null && options.getVariantAnnotationTables() == null) {
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

		// check whether user provided VCF tables IDs
		if (options.getVCFTables().isEmpty()) {
			throw new IllegalArgumentException(
					"Please specify VCF tables (e.g., VCFTables=genomics-public-data:platinum_genomes.variants )");
		}
		long startTime = System.currentTimeMillis();

		String queryString = "";
		boolean LegacySql=false;
		
		if (options.getSampleId().isEmpty()) {
			if (options.getGeneBasedAnnotation()) {
				if (options.getGeneBasedMinAnnotation()) {
					LOG.info("<============ Gene-based Annotation (mVCF) - Closest Genes (Min) ============>");
				} else {
					LOG.info("<============ Gene-based Annotation (mVCF) - Closest Genes (Range) ============>");
				}				
				queryString = BigQueryFunctions.prepareGeneBasedQueryConcatFields_mVCF_Range_Min_StandardSQL(options.getVCFTables(),
						options.getVCFCanonicalizeRefNames(), options.getGenericAnnotationTables(),
						options.getTranscriptCanonicalizeRefNames(), options.getProximityThreshold(),
						options.getOnlyIntrogenic(), options.getOutputFormatTable(), options.getGeneBasedMinAnnotation());
				
			} else {// Variant-based or Interval-based annotation
				LOG.info("<============ Variant-based Annotation OR/AND Interval-based Annotation (mVCF) ============>");
				queryString = BigQueryFunctions.prepareAnnotateVariantQueryConcatFields_mVCF(options.getVCFTables(),
						options.getVCFCanonicalizeRefNames(), options.getGenericAnnotationTables(),
						options.getTranscriptCanonicalizeRefNames(), options.getVariantAnnotationTables(),
						options.getVariantAnnotationCanonicalizeRefNames(), false);
				LegacySql=true;
			}
		} else { // e.g., LP6005038-DNA_H11
			if (options.getGeneBasedAnnotation()) {
				if (options.getGeneBasedMinAnnotation()) {
					LOG.info("<============ Gene-based Annotation (VCF - One Sample) - Closest Genes (Min) ============>");
				} else {
					LOG.info("<============ Gene-based Annotation (VCF - One Sample) - Closest Genes (Range) ============>");			
				}
				queryString = BigQueryFunctions.prepareGeneBasedQueryConcatFields_Range_Min_StandardSQL(
						options.getVCFTables(), options.getVCFCanonicalizeRefNames(),
						options.getGenericAnnotationTables(), options.getTranscriptCanonicalizeRefNames(),
						options.getSampleId(), options.getProximityThreshold(), options.getOnlyIntrogenic(), 
						options.getOutputFormatTable(), options.getGeneBasedMinAnnotation());
			} else {
				LOG.info("<============ Variant-based Annotation OR/AND Interval-based Annotation (VCF - One Sample) ============>");
				queryString = BigQueryFunctions.prepareAnnotateVariantQueryWithSampleNames(options.getVCFTables(),
						options.getVCFCanonicalizeRefNames(), options.getGenericAnnotationTables(),
						options.getTranscriptCanonicalizeRefNames(), options.getVariantAnnotationTables(),
						options.getVariantAnnotationCanonicalizeRefNames(), options.getSampleId(),
						options.getOutputFormatTable());
				LegacySql=true;
			}
		}

		LOG.info("Query: " + queryString);

		////////////////////////////////// STEP1/////////////////////////////////////
		////////////////////////////////// Joins/////////////////////////////////////
		runQuery(queryString, options.getBigQueryDatasetId(), options.getOutputBigQueryTable(), true,
				options.getMaximumBillingTier(), LegacySql);

		long tempEstimatedTime = System.currentTimeMillis() - startTime;
		LOG.info("Execution Time for Join Query: " + tempEstimatedTime);

		if (!options.getOutputFormatTable()) {

			////////////////////////////////// STEP2////////////////////////////////////
			////////////////////////////////// Sort/////////////////////////////////////
			startTime = System.currentTimeMillis();
			runSort();
			tempEstimatedTime = System.currentTimeMillis() - startTime;
			LOG.info("Execution Time for Sort Query: " + tempEstimatedTime);

			///////////////// STEP3: Delete the Intermediate
			///////////////// Table///////////////////////////
			//TODO: Add a new condition here
			BigQueryFunctions.deleteTable(options.getBigQueryDatasetId(), options.getOutputBigQueryTable());
		}
	}

	private static void runQuery(String queryString, String bigQueryDataset, String outputBigQueryTable,
			boolean allowLargeResults, int maximumBillingTier, boolean LegacySql) {

		try {
			BigQueryFunctions.runQueryPermanentTable(queryString, bigQueryDataset, outputBigQueryTable,
					allowLargeResults, maximumBillingTier, LegacySql);
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
										for (TableRow row : records) {
											String temp = ""; // for every row, temp would be empty
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
											if (oldRow == null) // if this is the first row
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
													hmap.put((index + listString.length), Annotation[1]); // this case
																											// is when
																											// we have
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
											int chrm = (int) (key / 1000000000); //9 digits padding
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
									
									
								//Solution: When there is not much sapce available -> remember if you do this 
									// Then it would take much longer, that means you have to pay more. So, always better 
									// To reserve large memory instances to handle these cases.

//									Iterable<KV<Integer, Iterable<KV<Long, Iterable<String>>>>> x = c.element()
//											.getValue();
//
//										
//									for (int chrm = 1; chrm < 26; chrm++) {
//										for (KV<Integer, Iterable<KV<Long, Iterable<String>>>> ChromLevel : x) {
//											LOG.warning("Chrm: " + chrm + " ChromLevel.getKey().intValue(): " + ChromLevel.getKey().intValue());
//											if(Integer.compare(chrm, ChromLevel.getKey().intValue()) == 0) {
//												for (KV<Long, Iterable<String>> BinLevel : ChromLevel.getValue()) {
//													for (String ItemLevel : BinLevel.getValue()) {
//															c.output(ItemLevel);
//													}
//												}
//											}
//										}
//									}
									
									

									//This is another method for handling this case,  	
//									HashMap<Integer, Iterable<KV<Long, Iterable<String>>>> hmap = 
//											new HashMap<Integer, Iterable<KV<Long, Iterable<String>>>>();
//
//									Iterable<KV<Integer, Iterable<KV<Long, Iterable<String>>>>> x = c.element()
//											.getValue();
//
//										
//									//for (int chrm = 1; chrm < 26; chrm++) {
//										for (KV<Integer, Iterable<KV<Long, Iterable<String>>>> ChromLevel : x) {
//											hmap.put(ChromLevel.getKey().intValue(), ChromLevel.getValue());
//										}
//																			    	
//									    	for (int chrm = 1; chrm < 26; chrm++) {
//									    		if(hmap.containsKey(chrm)) {
//										    		for (KV<Long, Iterable<String>> BinLevel : hmap.get(chrm)) {
//														for (String ItemLevel : BinLevel.getValue()) {
//																c.output(ItemLevel);
//														}
//													}
//											}
//									    	}	
									
									
								}
							}))
					.apply("VCF", TextIO.write().to(options.getBucketAddrAnnotatedVCF()));

			p.run().waitUntilFinish();

			String Header = "";
			if (options.getVariantAnnotationTables() != null) {
				String[] VariantAnnotationTables = options.getVariantAnnotationTables().split(",");
				for (int index = 0; index < VariantAnnotationTables.length; index++) {

					/*
					 * Example: gbsc-gcp-project-cba:PublicAnnotationSets.hg19_GME:GME_AF
					 * :GME_NWA:GME_NEA ProjectId: gbsc-gcp-project-cba DatasetId:
					 * PublicAnnotationSets TableId: hg19_GME Features: GME_AF:GME_NWA:GME_NEA
					 */

					String[] TableInfo = VariantAnnotationTables[index].split(":");
					Header += "<" + TableInfo[1].split("\\.")[1] + "," + (index + 1) + ">";
					if (index + 1 < VariantAnnotationTables.length)
						Header += "\t";
				}
			}

			if (options.getGenericAnnotationTables() != null) {
				String[] TranscriptAnnotationTables = options.getGenericAnnotationTables().split(",");
				for (int index = 0; index < TranscriptAnnotationTables.length; index++) {

					String[] TableInfo = TranscriptAnnotationTables[index].split(":");
					Header += "<" + TableInfo[1].split("\\.")[1] + "," + (index + 1) + ">";
					if (index + 1 < TranscriptAnnotationTables.length)
						Header += "\t";
				}
			}

			Path VCF_Filename = Paths.get(options.getBucketAddrAnnotatedVCF());
			LOG.info("");
			LOG.info("");
			LOG.info("[INFO]------------------------------------------------------------------------\n"
					+ "Header: \n\n " + Header + "\n\n"
					+ " [INFO] To download the annotated VCF file from Google Cloud, run the following command:\n"
					+ "\t ~: gsutil cat " + options.getBucketAddrAnnotatedVCF() + "* > "
					+ VCF_Filename.getFileName().toString() + "\n" + "\n"
					+ "[INFO] To remove the output files from the cloud storage run the following command:\n"
					+ "\t ~: gsutil rm " + options.getBucketAddrAnnotatedVCF() + "* \n"
					+ "[INFO] ------------------------------------------------------------------------ \n\n");
		} else {

			////////////////////////////////// STEP2: Sort Per chromosome in BigQuery ->
			////////////////////////////////// Output is a local
			////////////////////////////////// file/////////////////////////////////////
			try {
				BigQueryFunctions.sortByBin(options.getProject(), options.getBigQueryDatasetId(),
						options.getOutputBigQueryTable(), options.getLocalOutputFilePath(), options.getBinSize());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	static final class BinVariantsFn extends DoFn<TableRow, KV<Long, TableRow>> {
		/**
		 * This function assign a unique ID to each variant chrId+start => 9 digits It
		 * take cares of the padding as well. It assigns 23, 24, and 24 to chromosme X,
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
