package com.cloudera.services.avro;

import org.apache.avro.Schema;

public class DataTypeConverter {

	public static Object convert(Object inputValue, Schema schema) {
		    if (inputValue == null) {
		      return null;
		    }
		    
		    String value = inputValue.toString();
		    
		    switch (firstNonNullType(schema)) {
		      case STRING:
		        return value;
		      case LONG:
		        return Long.valueOf(value);
		      case INT:
		        return Integer.valueOf(value);
		      case DOUBLE:
		        return Double.valueOf(value);
		      case FLOAT:
		        return Float.valueOf(value);
		      case BOOLEAN:
		        return Boolean.valueOf(value);
		      default:
		        throw new IllegalArgumentException("Unsupported schema: "  + schema);
		    }
		  }
	
	private static Schema.Type firstNonNullType(Schema schema) {
		    if (schema.getType() == Schema.Type.UNION) {
		      for (Schema s : schema.getTypes()) {
		        if (s.getType() != Schema.Type.NULL) {
		          return s.getType();
		        }
		      }
		      throw new IllegalArgumentException(
		          "Schema has only null types: "  + schema);
		    } else {
		      return schema.getType();
		    }
		  }
}
