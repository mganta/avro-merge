This utility merges various avro files into a common schema based on a custom mapping supplied

To Run

hadoop jar \
avro-merge-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
com.cloudera.services.avro.MergeAvroData \
--files FieldMappings.json \
avro_data_paths.txt  /user/foo/merged_output

