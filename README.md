This utility merges various avro files into a common schema based on a custom mapping supplied

To Run

hadoop jar \
avro-merge-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
com.cloudera.services.avro.MergeAvroData \
--files field_mappings.properties \
avro_data_paths.txt field_mappings.properties /user/foo/merged_output