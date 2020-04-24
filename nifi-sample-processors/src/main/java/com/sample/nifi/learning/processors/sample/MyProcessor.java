/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sample.nifi.learning.processors.sample;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.Page;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

@Tags({ "example" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })

public class MyProcessor extends AbstractBigQueryProcessor {

	private ComponentLog logger;

	static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder().name("Bigquery Table")
	    .description("The table id where store the data. The table must be exist on bigquery").required(true)
	    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	static final PropertyDescriptor OUT_TABLE = new PropertyDescriptor.Builder().name("Output Bigquery Table")
	    .description("The table id where store the data. The table must be exist on bigquery").required(true)
	    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	static final PropertyDescriptor DATASET = new PropertyDescriptor.Builder().name("Bigquery Dataset")
	    .description("The dataset id where find the table. The dataset must be exist on bigquery").required(true)
	    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder().name("Bigquery Insert Batch Size")
	    .description("The max number of flow files to insert in a table in one request. "
	        + "Default is 500 as recommended by bigquery quota documentation.")
	    .required(true).defaultValue("500").addValidator(StandardValidators.INTEGER_VALIDATOR).build();

	private List<String> formatBigqueryErrors(List<BigQueryError> errors) {
		List<String> errorsString = new ArrayList<>();
		for (BigQueryError error : errors) {
			errorsString.add(error.toString());
		}
		return errorsString;
	}

	private String created_at() {
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
		df.setTimeZone(tz);
		return df.format(new Date());
	}

	private JSONObject parseJson(InputStream jsonStream) throws JsonIOException, JsonSyntaxException, IOException {
		return new JSONObject(JsonParserUtils.fromStream(jsonStream).toString());
	}

	public static final List<PropertyDescriptor> properties = Collections
	    .unmodifiableList(Arrays.asList(SERVICE_ACCOUNT_CREDENTIALS_JSON, READ_TIMEOUT, CONNECTION_TIMEOUT, PROJECT,
	        DATASET, TABLE, OUT_TABLE, BATCH_SIZE));

	public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor.Builder().name("MY_PROPERTY")
	    .displayName("My property").description("Example Property").required(true)
	    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship MY_RELATIONSHIP = new Relationship.Builder().name("MY_RELATIONSHIP")
	    .description("Example relationship").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(MY_PROPERTY);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(MY_RELATIONSHIP);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	private Integer batchSize(ProcessContext context) {
		PropertyValue batchSizeProperty = context.getProperty(BATCH_SIZE);
		return batchSizeProperty.asInteger();
	}

	private void readFromTable(final ProcessContext context, final ProcessSession session) {
		TableReference tableSpec = new TableReference().setProjectId("clouddataflow-readonly").setDatasetId("samples")
		    .setTableId("weather_stations");
	}

	private Object convertField(Field fieldSchema, FieldValue field) {
		if (field.isNull()) {
			return null;
		}
		switch (field.getAttribute()) {
		case PRIMITIVE:
			switch (fieldSchema.getType().getValue()) {
			case BOOLEAN:
				return field.getBooleanValue();
			case FLOAT:
				return field.getDoubleValue();
			case INTEGER:
				return field.getLongValue();
			case STRING:
				return field.getStringValue();
			case TIMESTAMP:
				return field.getTimestampValue();
			default:
				throw new RuntimeException("Cannot convert primitive field type " + fieldSchema.getType());
			}
		case REPEATED:
			List<Object> result = new ArrayList<>();
			for (FieldValue arrayField : field.getRepeatedValue()) {
				result.add(convertField(fieldSchema, arrayField));
			}
			return result;
		case RECORD:
			List<Field> recordSchemas = fieldSchema.getFields();
			List<FieldValue> recordFields = field.getRecordValue();
			return convertRow(recordSchemas, recordFields);
		default:
			throw new RuntimeException("Unknown field attribute: " + field.getAttribute());
		}
	}

	private List<Object> convertRow(List<Field> rowSchema, List<FieldValue> row) {
		List<Object> result = new ArrayList<>();
		assert (rowSchema.size() == row.size());

		for (int i = 0; i < rowSchema.size(); i++) {
			result.add(convertField(rowSchema.get(i), row.get(i)));
		}

		return result;
	}

	private List<List<Object>> readAllRows(String dataset, String tableName) {
		Table table = bigQuery.getTable(dataset, tableName);
		Schema schema = table.getDefinition().getSchema();
		Page<List<FieldValue>> page = table.list();

		List<List<Object>> rows = new ArrayList<>();
		while (page != null) {
			for (List<FieldValue> row : page.getValues()) {
				rows.add(convertRow(schema.getFields(), row));
			}
			page = page.getNextPage();
		}
		return rows;
	}

	public static Map<String, Object> toMap(JSONObject object) throws JSONException {
		Map<String, Object> map = new HashMap<String, Object>();

		Iterator<String> keysItr = object.keys();
		while (keysItr.hasNext()) {
			String key = keysItr.next();
			Object value = object.get(key);

			if (value instanceof JSONArray) {
				value = toList((JSONArray) value);
			}

			else if (value instanceof JSONObject) {
				value = toMap((JSONObject) value);
			}
			map.put(key, value);
		}
		return map;
	}

	public static List<Object> toList(JSONArray array) {
		List<Object> list = new ArrayList<Object>();
		for (int i = 0; i < array.length(); i++) {
			Object value = array.get(i);
			if (value instanceof JSONArray) {
				value = toList((JSONArray) value);
			}

			else if (value instanceof JSONObject) {
				value = toMap((JSONObject) value);
			}
			list.add(value);
		}
		return list;
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		/*
		 * FlowFile flowFile = session.get(); if ( flowFile == null ) { return; }
		 */

		logger = getLogger();

		FlowFile input = null;
		if (context.hasIncomingConnection()) {
			input = session.get();
			if (input == null && context.hasNonLoopConnection()) {
				return;
			}
		}

		Integer batchSize = batchSize(context);
		List<FlowFile> flowFiles = session.get(batchSize);
		List<InsertAllRequest.RowToInsert> rowsToInsert = new ArrayList<>();
		List<FlowFile> flowFilesToInsert = new ArrayList<>();
		List<JSONObject> listOfContent = new ArrayList<>();

		final String table = context.getProperty(OUT_TABLE).getValue();
		final String inputTable = context.getProperty(TABLE).getValue();
		final String dataset = context.getProperty(DATASET).getValue();

		List<List<Object>> allRecords = readAllRows(dataset, inputTable);

		for (FlowFile flowFile : flowFiles) {
			try {
				JSONObject jsonDocument = parseJson(session.read(flowFile));
				InsertAllRequest.RowToInsert rowToInsert = InsertAllRequest.RowToInsert.of(toMap(jsonDocument));
				rowsToInsert.add(rowToInsert);
				flowFilesToInsert.add(flowFile);
				listOfContent.add(jsonDocument);
			} catch (IOException e) {
				getLogger().error("IOException while reading JSON item: " + e.getMessage());
				flowFile = session.putAttribute(flowFile, "error_message",
				    "IOException while reading JSON item: " + e.getMessage());
				session.transfer(flowFile, REL_FAILURE);
			} catch (JsonIOException e) {
				getLogger().error("JsonIOException while reading JSON item: " + e.getMessage());
				flowFile = session.putAttribute(flowFile, "error_message",
				    "JsonIOException while reading JSON item: " + e.getMessage());
				session.transfer(flowFile, REL_FAILURE);
			} catch (JsonSyntaxException e) {
				getLogger().error("JsonSyntaxException while reading JSON item: " + e.getMessage());
				flowFile = session.putAttribute(flowFile, "error_message",
				    "JsonSyntaxException while reading JSON item: " + e.getMessage());
				session.transfer(flowFile, REL_FAILURE);
			}
		}

		if (!rowsToInsert.isEmpty()) {
			InsertAllRequest insertAllRequest = InsertAllRequest.of(dataset, table, rowsToInsert);
			InsertAllResponse insertAllResponse = getBigQuery().insertAll(insertAllRequest);

			for (int index = 0; index < flowFilesToInsert.size(); index++) {
				List<BigQueryError> errors = insertAllResponse.getErrorsFor(index);
				FlowFile flowFile = flowFilesToInsert.get(index);

				if (errors.isEmpty()) {
					session.transfer(flowFile, REL_SUCCESS);
				} else {
					String content = listOfContent.get(index).toString();

					flowFile = session.write(flowFile, new OutputStreamCallback() {
						@Override
						public void process(OutputStream out) throws IOException {
							JSONObject json = new JSONObject();

							json.put("errors", formatBigqueryErrors(errors));
							json.put("content", content);
							json.put("created_at", created_at());

							out.write(json.toString().getBytes());
						}
					});
					session.transfer(flowFile, REL_FAILURE);
				}
			}

			// TODO implement
		}
	}
}
