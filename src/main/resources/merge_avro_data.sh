hadoop jar \
avro-merge-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
com.cloudera.services.avro.MergeAvroData \
--files field_mappings.properties \
avro_data_paths.txt field_mappings.properties /user/root/merged_output