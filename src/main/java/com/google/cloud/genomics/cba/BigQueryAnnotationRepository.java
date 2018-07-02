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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * <h1>BigQuery Annotation Repository</h1> This class helps to store information
 * about the imported annotation reference files.
 * 
 * @param BigQueryDataset
 *            The ID of the Google BigQuery Dataset name that the output
 *            AnnotationSet will be posted.
 * 
 * @param annotationInputTextBucketAddr
 *            This provides the URI of inputfile which contains annotations.
 * 
 * @param build
 *            This provides the build version of reference annotation datasets
 *            (hg18/hg19/hg38)
 * 
 * @version 1.0
 * @since 2018-02-01
 */

public class BigQueryAnnotationRepository {

	public static interface Options extends GenomicsOptions {

		@Description("This provides the header for the Annotations. This is a required field. "
				+ "(It must start w/ the following fields: \"Chrom,start,end,ref,alter,all other fileds\")")
		@Default.String("")
		String getHeader();

		void setHeader(String header);

		@Description("This provides BigQuery Dataset ID.")
		@Default.String("")
		String getBigQueryDatasetId();

		void setBigQueryDatasetId(String BigQueryDatasetId);

		@Description("This provides BigQuery Table.")
		@Default.String("")
		String getBigQueryAnnotationSetTableId();

		void setBigQueryAnnotationSetTableId(String BigQueryAnnotationSetTableId);

		@Description("This provides whether the annotation is Varaiant or Transcript. This is a required field.")
		@Default.String("")
		String getAnnotationType();

		void setAnnotationType(String VariantAnnotation);

		@Description("This provides the version annotationset. This is a required field.")
		@Default.String("")
		String getAnnotationSetVersion();

		void setAnnotationSetVersion(String AnnotationSetVersion);

		@Description("This provides more info about the annotationset. This is an optional filed.")
		@Default.String("")
		String getAnnotationSetInfo();

		void setAnnotationSetInfo(String AnnotationSetInfo);

		@Description("This provides assemblyId. This is a required field.")
		@Default.String("")
		String getAssemblyId();

		void setAssemblyId(String AssemblyId);

		@Description("This provides the number of workers. This is a required filed.")
		int getNumWorkers();

		void setNumWorkers(int value);

		@Description("This provides whether AnnotationSetList table exist or not. If not it will create the table.")
		@Default.Boolean(false)
		boolean getCreateAnnotationSetListTable();

		void setCreateAnnotationSetListTable(boolean value);

		@Description("Users can list available annotation datasets.")
		@Default.Boolean(true)
		boolean getListAnnotationDatasets();

		void setListAnnotationDatasets(boolean value);

		@Description("Users can ask the system to print all fields (AnnotationSetName, "
				+ "AnnotationSetVersion, AnnotationSetType, AnnotationSetFields, CreationDate, AnnotationSetSize, info, Build).")
		@Default.Boolean(false)
		boolean getPrintAllFields();

		void setPrintAllFields(boolean value);

		@Description("Users can specify the build name. Deafult value is empty - This will return back all annotationdatasets")
		@Default.String("")
		String getAnnotationDatasetBuild();

		void setAnnotationDatasetBuild(String value);

		@Description("Users can submit a keyword, and the system search to returns information about any matched annotation name "
				+ "- This query uses LIKE %<Keyword>%. Deafult value is empty - This will return back all annotationdatasets")
		@Default.String("")
		String getSearchKeyword();

		void setSearchKeyword(String value);
		

	}

	private static Options options;
	private static boolean DEBUG;
	private static final Logger LOG = Logger.getLogger(BigQueryAnnotationRepository.class.getName());

	public static void run(String[] args) throws GeneralSecurityException, IOException {

		PipelineOptionsFactory.register(Options.class);
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		if (options.getBigQueryDatasetId().isEmpty()) {
			throw new IllegalArgumentException("BigQuery DatasetId must be specified");
		}

		if (options.getCreateAnnotationSetListTable()) {
			createAnnotationSetList();
			return;
		}

		if (options.getListAnnotationDatasets()) {
			try {
				BigQueryFunctions.listAnnotationDatasets(options.getBigQueryDatasetId(),
						"AnnotationList", options.getAnnotationType(), options.getAnnotationDatasetBuild(), 
						options.getPrintAllFields(), options.getSearchKeyword());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	static TableReference getTable(String projectId, String datasetId, String tableName) {
		TableReference table = new TableReference();
		table.setDatasetId(datasetId);
		table.setProjectId(projectId);
		table.setTableId(tableName);
		return table;
	}

	/**
	 * <h1>This function add a new table called AnnotationList. AnnotationList is a
	 * table containing the information about annotation sets.
	 *
	 */
	public static void createAnnotationSetList() {

		BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();
		BigQuery bigquery = optionsBuilder.build().getService();

		Table existTable = bigquery.getTable(options.getBigQueryDatasetId(), "AnnotationList");

		if (existTable == null) {
			List<Field> fields = new ArrayList<Field>();
			fields.add(Field.of("AnnotationSetName", Field.Type.string()));
			fields.add(Field.of("AnnotationSetVersion", Field.Type.string()));
			fields.add(Field.of("AnnotationSetType", Field.Type.string()));
			fields.add(Field.of("AnnotationSetFields", Field.Type.string()));
			fields.add(Field.of("CreationDate", Field.Type.string()));
			fields.add(Field.of("Build", Field.Type.string()));
			fields.add(Field.of("AnnotationSetSize", Field.Type.string()));
			fields.add(Field.of("Info", Field.Type.string()));
			Schema schema = Schema.of(fields);
			TableId tableId = TableId.of(options.getBigQueryDatasetId(), "AnnotationList");

			TableDefinition tableDefinition = StandardTableDefinition.of(schema);
			TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
			bigquery.create(tableInfo);
			LOG.info("### Successfully created " + options.getProject() + ":" + options.getBigQueryDatasetId()
					+ ".AnnotationList table.");

			Map<String, Object> rowContent = new HashMap<>();
			rowContent.put("AnnotationSetName", "Test");
			rowContent.put("AnnotationSetVersion", "Test");
			rowContent.put("AnnotationSetType", "Test");
			rowContent.put("AnnotationSetFields", "Test");
			rowContent.put("CreationDate", "Test");
			rowContent.put("Build", "Test");
			rowContent.put("AnnotationSetSize", "Test");
			rowContent.put("Info", "Test");
			InsertAllResponse response;
			response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId).addRow("rowId", rowContent).build());
			if (DEBUG) {
				LOG.info(response.toString());
			}
		} else
			LOG.severe("### Table \"AnnotationList\" exists");

	}

	/**
	 * <h1>This function adds information about the annotation set to the
	 * AnnotationList table. AnnotationList is a table containing the information
	 * about annotation sets
	 *
	 */
	static void addToAnnotationSetList(long tempEstimatedTime) {

		BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();
		BigQuery bigquery = optionsBuilder.build().getService();

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		TableId tableId = TableId.of(options.getBigQueryDatasetId(), "AnnotationList");
		Map<String, Object> rowContent = new HashMap<>();
		rowContent.put("AnnotationSetName", options.getBigQueryAnnotationSetTableId());
		rowContent.put("AnnotationSetVersion", options.getAnnotationSetVersion());
		rowContent.put("AnnotationSetType", options.getAnnotationType());
		rowContent.put("AnnotationSetFields", options.getHeader());
		rowContent.put("CreationDate", dateFormat.format(date).toString());
		rowContent.put("Build", options.getAssemblyId());

		QueryResponse rs = BigQueryFunctions.runquery(BigQueryFunctions.countQuery(options.getProject(),
				options.getBigQueryDatasetId(), options.getBigQueryAnnotationSetTableId()));
		QueryResult result = rs.getResult();
		String numAnnotations = "0";
		if (result != null)
			for (List<FieldValue> row : result.iterateAll()) {
				numAnnotations = row.get(0).getValue().toString();
			}

		LOG.info("### number of annotations added: " + numAnnotations);
		rowContent.put("AnnotationSetSize", numAnnotations);
		if (options.getNumWorkers() != 0)
			rowContent.put("Info", options.getAnnotationSetInfo() + "\t Number of Instances: " + options.getNumWorkers()
					+ " ExecutionTime (msec): " + tempEstimatedTime);
		else
			rowContent.put("Info", options.getAnnotationSetInfo()
					+ "\t Number of Instances: AutoScaling ExecutionTime (msec): " + tempEstimatedTime);

		InsertAllResponse response;
		response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId).addRow(rowContent).build());

		//System.out.println(response.toString());
		if (!response.getInsertErrors().isEmpty())
			LOG.severe("### Error in inserting a new table to AnnotationSetList Table: "
					+ response.getInsertErrors().toString());
		else
			LOG.info("### Successfully added a new annotationSet table: " + rowContent.toString());
	}
}

/*
 * //Delete Query #standardSQL SELECT EXCEPT(HumanAssembly), HumanAssembly AS
 * Build FROM `gbsc-gcp-project-cba.annotation.AnnotationList`
 * 
 * #standardSQL DELETE FROM
 * `gbsc-gcp-project-cba.PublicAnnotationSets.AnnotationList` WHERE
 * AnnotationSetName="Test"
 * 
 */
