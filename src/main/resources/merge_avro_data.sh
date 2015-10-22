hadoop jar \
avro-merge-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
com.cloudera.services.avro.MergeAvroData \
--files FieldMappings.json \
avro_data_paths.txt /user/root/merged_output