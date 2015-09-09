package com.cloudera.services.avro;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.services.hbase.CombinedPWData;

public class MergeAvroData extends Configured implements Tool {
	private Configuration config = new Configuration();

	public Configuration getConf() {
		return config;
	}

	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		if (args.length != 3) {
			System.err
					.printf("Usage: %s [generic options] <input_paths_file> <mappings_file_name> <output_path> \n",
							getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		config.set("MappingsFilename", args[1]);

		Job job = Job.getInstance(config);

		job.setJarByClass(MergeAvroData.class);
		job.setJobName("MergeAvroJob");

		job.setInputFormatClass(AvroKeyInputFormat.class);

		// generic input
		job.setMapperClass(GenericAvroMapper.class);
		job.setMapOutputKeyClass(AvroKeyOutputFormat.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setNumReduceTasks(0);

		// clear out output folder if it exists
		FileSystem fs = FileSystem.get(config);
		if (fs.exists(new Path(args[2])))
			fs.delete(new Path(args[2]), true);

		//read all the input paths to various avro data files
		List<String> lines = Files.readAllLines(Paths.get(args[0]),
				Charset.defaultCharset());
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < lines.size() - 1; i++) {
			sb.append(lines.get(i));
			sb.append(",");
		}

		if (lines.size() > 0) {
			sb.append(lines.get(lines.size() - 1));
		}

		//set input paths
		AvroKeyInputFormat.addInputPaths(job, sb.toString());

		//set output path and other params
		AvroKeyOutputFormat.setOutputPath(job, new Path(args[2]));
		AvroKeyOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		AvroKeyOutputFormat.setCompressOutput(job, true);
		AvroJob.setOutputKeySchema(job, CombinedPWData.SCHEMA$);

		//run the job
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		MergeAvroData processor = new MergeAvroData();
		String[] otherArgs = new GenericOptionsParser(processor.getConf(), args)
				.getRemainingArgs();
		System.exit(ToolRunner.run(processor.getConf(), processor, otherArgs));
	}

}
