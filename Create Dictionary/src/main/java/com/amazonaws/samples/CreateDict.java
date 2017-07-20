package com.amazonaws.samples;
import java.io.IOException;
import org.apache.commons.codec.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class CreateDict {
	
	static Text uniqueWordsIndicator = new Text("^^&&***TOTALNUMBEROFUNIQUEWORDS***&&^^");
	/*	=======================================	
	 * 	===============  Mapper ===============
	 *  ======================================= */
	public static class CreateDictMapper extends Mapper<Text, LongWritable, IntWritable, Text>{
		
		private final static IntWritable one = new IntWritable(1);
	    
	    @Override
	    public void map(Text word, LongWritable numOfTweets, Context context) throws IOException,  InterruptedException {
        	Text txt = new Text(word.toString() +"\t"+ numOfTweets.toString());
        	context.write(one, txt);
        	// one as key -> all values from all mappers goes to a single reducer, 
	    }
	}

	
	/*	=======================================	
	 * 	============== Reducer ================
	 *  ======================================= */
	 public static class CreateDictReducer extends Reducer<IntWritable, Text, Text, Text> {
		 
		@Override
		// single reducer gets all values because mapper sends everything under same key = "one"
		// reducer input = <one, "word \t numOfTweetsOccurances">
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			
			// create dictionary on hdfs - only single reducer so this happens only once
			Configuration conf = context.getConfiguration();
			String path = conf.get("dictionaryPath");
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream outStream = fs.create(new Path(path));
			
			long i = 0;
			for (Text wordTweetCount: values){
				String split[] = wordTweetCount.toString().split("\t"); // split by tab
				String word = split[0];
				String tweetCount = split[1];
				String toWrite = word + "\t" + i + "\t" + tweetCount + "\n";
				
				byte[] buffer = toWrite.getBytes(Charsets.UTF_8);
				outStream.write(buffer);
				context.write(new Text(word), new Text(i + "\t" + tweetCount)); 
				i++;
			}
		}
	}
	 
	 
	 
	 
	 /*	 =======================================	
	  *  =============== Main ==================
	  *  ======================================= */
	 public static void main(String[] args) throws Exception {

	        Configuration conf = new Configuration();
	        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	        //conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization");
	        conf.set("dictionaryPath", args[3]);
	        Job job = Job.getInstance(conf, "Create Dict");
	        job.setJar("createDict.jar");
	        job.setMapperClass(CreateDictMapper.class);
	        job.setMapOutputKeyClass(IntWritable.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setReducerClass(CreateDictReducer.class);
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
	        TextOutputFormat.setOutputPath(job, new Path(args[2]));
	        Boolean success = job.waitForCompletion(true);
	        if(success){
	        	System.out.println("CreateDict: finished successfully");
	        	System.exit(0);
	        } else {
	        	System.out.println("CreateDict: failed");
	        	System.exit(1);
	        }
	 }

}
