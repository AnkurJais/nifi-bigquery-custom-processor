package com.sample.nifi.learning.processors.sample;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.google.cloud.Page;
import com.google.cloud.bigquery.BigQuery.TableDataListOption;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.TableId;

public class ReadBigQueryProcessor extends AbstractBigQueryProcessor {
	private ComponentLog logger;

	static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder().name("Bigquery Table")
	    .description("The table id where store the data. The table must be exist on bigquery").required(true)
	    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	static final PropertyDescriptor DATASET = new PropertyDescriptor.Builder().name("Bigquery Dataset")
	    .description("The dataset id where find the table. The dataset must be exist on bigquery").required(true)
	    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder().name("Bigquery Insert Batch Size")
	    .description("The max number of flow files to insert in a table in one request. "
	        + "Default is 500 as recommended by bigquery quota documentation.")
	    .required(true).defaultValue("500").addValidator(StandardValidators.INTEGER_VALIDATOR).build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("success").build();

	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
	    .description("FlowFiles are routed to failure relationship").build();

	public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor.Builder().name("MY_PROPERTY")
	    .displayName("My property").description("Example Property").required(true)
	    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays
	    .asList(SERVICE_ACCOUNT_CREDENTIALS_JSON, READ_TIMEOUT, CONNECTION_TIMEOUT, PROJECT, DATASET, TABLE, BATCH_SIZE));

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	public Iterator<List<FieldValue>> listTableData(String datasetName, String tableName) {
		TableId tableIdObject = TableId.of(datasetName, tableName);
		Page<List<FieldValue>> tableData = bigQuery.listTableData(tableIdObject, TableDataListOption.pageSize(100));
		Iterator<List<FieldValue>> rowIterator = tableData.iterateAll();
		return rowIterator;
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		// TODO Auto-generated method stub
		String tableName = context.getProperty(TABLE).getValue();
		String dataset = context.getProperty(DATASET).getValue();
		Iterator<List<FieldValue>> tableData = listTableData(dataset, tableName);
		while (tableData.hasNext()) {
			FlowFile flowFile = session.create();
			try {
				flowFile = session.write(flowFile, new OutputStreamCallback() {
					@Override
					public void process(OutputStream out) throws IOException {
						// TODO Auto-generated method stub
						ByteArrayOutputStream bos = new ByteArrayOutputStream();
						ObjectOutputStream oos = new ObjectOutputStream(bos);
						oos.writeObject(tableData.next());
						oos.flush();
						out.write(bos.toByteArray());
					}
				});
			} catch (Exception e) {
				// TODO: handle exception
				getLogger().error("IOException while reading BigQuery item: " + e.getMessage());
				flowFile = session.putAttribute(flowFile, "error_message",
				    "IOException while reading BigQuery item: " + e.getMessage());
				session.transfer(flowFile, REL_FAILURE);
			}
			session.getProvenanceReporter().create(flowFile);
			session.transfer(flowFile, SUCCESS);
		}
	}

}
