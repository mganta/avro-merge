package com.cloudera.services.avro;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

public class GenericAvroMapper
		extends
		Mapper<AvroWrapper<GenericRecord>, NullWritable, AvroKey<CombinedPWData>, NullWritable> {
	private static final String MAPPINGS_FILENAME = "MappingsFilename";
	public static final Map<String, FieldMappings> fieldMappings = new HashMap<String, FieldMappings>();

	@Override
	protected void setup(Context context) {
		// get the custom mappings filename from conf
		String mappingsFilename = context.getConfiguration().get(
				MAPPINGS_FILENAME);
		try {
			Gson gson = new Gson();
			FieldMappings[] mappings = gson.fromJson(new FileReader(
					mappingsFilename), FieldMappings[].class);

			for (FieldMappings mapping : mappings) 
				fieldMappings.put(mapping.getSourceColumn(), mapping);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/***
	 * Read different types of avro files and create one specific avro
	 */
	@Override
	public void map(AvroWrapper<GenericRecord> key, NullWritable value,
			Context context) throws IOException, InterruptedException {
		// read the record, get its schema, key & values
		GenericRecord avroRecord = key.datum();
		Schema avroSchema = avroRecord.getSchema();
		String schemaName = avroSchema.getName();
		List<Field> fields = avroSchema.getFields();
		CombinedPWData combinedRecord = new CombinedPWData();
		Map<String, Object> othersMap = new HashMap<String, Object>();
	
		// for each field check if there is a custom mapping else pass it thru
		for (Field field : fields) {
			String fieldName = field.name();
			Object fieldValue = avroRecord.get(fieldName);
			if (fieldValue != null) {
				if (fieldMappings.containsKey(fieldName)) {
					if (fieldMappings.get(fieldName).getSourceTable().equalsIgnoreCase(schemaName)) {
						combinedRecord.put(fieldMappings.get(fieldName).getTargetColumn(), DataTypeConverter.convert(fieldValue, combinedRecord.getSchema().getField(fieldMappings.get(fieldName).getTargetColumn()).schema()));
					} else {
						if(combinedRecord.getSchema().getField(fieldName) != null)	
							   combinedRecord.put(fieldName, DataTypeConverter.convert(fieldValue, combinedRecord.getSchema().getField(fieldName).schema()));
							else
					    othersMap.put(fieldName, DataTypeConverter.convert(fieldValue, Schema.create(Type.STRING)));
					}
				} else {
					if(combinedRecord.getSchema().getField(fieldName) != null)	
					   combinedRecord.put(fieldName, DataTypeConverter.convert(fieldValue, combinedRecord.getSchema().getField(fieldName).schema()));
					else
					    othersMap.put(fieldName, DataTypeConverter.convert(fieldValue, Schema.create(Type.STRING)));
				}
			}
		}
		
			combinedRecord.put("others", othersMap);;
			
		// write the results
		if (combinedRecord != null)
			context.write(new AvroKey<CombinedPWData>(combinedRecord), null);
	}
}
