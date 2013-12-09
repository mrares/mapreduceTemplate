package org.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.google.common.base.Splitter;

public class hive2text {

	public static class RowMapper extends MapReduceBase implements Mapper<BytesWritable, Text, Text, IntWritable> {

		private static final String SEPARATOR_FIELD = new String(new char[] {1});
		
		public void map(
				BytesWritable key,
				Text value,
				OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			
	    	String rowTextObject = value.toString();;

	    	Iterable<String> rowColumns = Splitter.on(SEPARATOR_FIELD).split(rowTextObject);
	    	
	    	java.util.Map<Integer, String> mp = new java.util.HashMap<Integer, String>();
	    	Integer k = 0;
			for(String s: rowColumns) {
				mp.put(k++, s);
			}
	    	
			output.collect(new Text(mp.get(1)), new IntWritable(Integer.valueOf(mp.get(0))));
		}

	}

	/**
	 * We assume the input directory is an external table created by hive
	 * Eg: CREATE EXTERNAL TABLE <table_name>(<table_column_definition>) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS SEQUENCEFILE LOCATION '<hdfs path>';
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: hadoop jar <jar file> " + WordCount.class.getName() + " <source dir> <destination dir>");
			return;
		}
		
		JobConf conf = new JobConf(hive2text.class);
		Path inputDir = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		
		conf.setJobName("hive2text");

		conf.setInputFormat(SequenceFileInputFormat.class);

		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(RowMapper.class);

		FileInputFormat.setInputPaths(conf, inputDir);
		FileOutputFormat.setOutputPath(conf, outputDir);

		JobClient.runJob(conf);
	}

}
