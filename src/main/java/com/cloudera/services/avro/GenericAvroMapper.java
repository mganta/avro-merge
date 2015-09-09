package com.cloudera.services.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudera.services.hbase.CombinedPWData;

public class GenericAvroMapper extends
		Mapper<AvroWrapper<GenericRecord>, NullWritable, AvroKey<CombinedPWData>, NullWritable> {
	private static final String MAPPINGS_FILENAME = "MappingsFilename";
	public static final Properties fieldMappings = new Properties();

	@Override
	protected void setup(Context context) {
		//get the custom mappings filename from conf
		String mappingsFilename = context.getConfiguration().get(MAPPINGS_FILENAME);
		try {
			//load it into a properties file
			fieldMappings.load(new FileInputStream(new File(mappingsFilename)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/***
	 * Read different types of avro files and create one specific avro
	 */
	@Override
	public void map(AvroWrapper<GenericRecord> key, NullWritable value,
			Context context) throws IOException, InterruptedException {
		//read the record, get its schema, key & values
		GenericRecord avroRecord = key.datum();
		Schema avroSchema = avroRecord.getSchema();
		String schemaName = avroSchema.getName();
		List<Field> fields = avroSchema.getFields();
		CombinedPWData combinedRecord = new CombinedPWData();

		//for each field check if there is a custom mapping else pass it thru
		for (Field field : fields) {
			String fieldName = field.name();
			Object fieldValue = avroRecord.get(fieldName);
			if (fieldValue != null) {
				String avroTransformName = fieldMappings.getProperty(schemaName
						+ "," + fieldName);
				if (avroTransformName != null) {
					String[] parts = avroTransformName.split(",");
					if (parts.length == 2) {
						String combinedSchemaName = parts[0];
						String combinedColumnName = parts[1];
						if (combinedSchemaName.equals(CombinedPWData.SCHEMA$.getName()))
							combinedRecord.put(combinedColumnName, fieldValue);
					}
				} else {
					combinedRecord.put(fieldName, fieldValue);
				}
			}
		}

		//write the results
		if (combinedRecord != null)
			context.write(new AvroKey<CombinedPWData>(combinedRecord), null);
	}
}
