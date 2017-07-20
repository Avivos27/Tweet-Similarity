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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class CreateTweetVectorTable {

	/*	=======================================	
	 * 	===============  Mapper ===============
	 *  ======================================= */
	public static class CreateTweetVectorTableMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		
		private final static IntWritable one = new IntWritable(1);
	    
	    @Override
	    // maps sends all information to one reducer for indexing
	    public void map(LongWritable tweetId, Text vectorAndTweetDesc, Context context) throws IOException,  InterruptedException {
	    	context.write(one,new Text(tweetId + "\t" + vectorAndTweetDesc));
	    }
	}

	
	/*	=======================================	
	 * 	============== Reducer ================
	 *  ======================================= */
	 public static class CreateTweetVectorTableReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
		 
		@Override
		// single reducer gets all values because mapper sends everything under same key = "one"
		// reducer input = <one, tweetId \t tweetVector \t tweetDesc> 
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			
			// create a new table in hdfs with the following entries for each tweet:
			// lineEntry = tweetId \t vector \t text
			Configuration conf = context.getConfiguration();
			String path = conf.get("hdfsPath");
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream outStream = fs.create(new Path(path));
			for (Text tweetIdVectorDesc: values){
				String tweetIdVectorDescArray[] = tweetIdVectorDesc.toString().split("\t");
				long id = Long.parseLong(tweetIdVectorDescArray[0]);
				String vector = tweetIdVectorDescArray[1];
				String text = tweetIdVectorDescArray[2];
				text = text.replace("\r", " ").replace("\n", " ").replace("\t", " ");
				
				// writing line by line to hdfs:///indexVectorList/ 
				String line = id +"\t"+ vector + "\t" + text + "\n"; 
				byte[] buffer = line.getBytes(Charsets.UTF_8);
				outStream.write(buffer);
				
				// emit output
				Text tweetVectorDesc = new Text(tweetIdVectorDesc.toString().split("\t",2)[1]);
				context.write(new LongWritable(id) , tweetVectorDesc);
				// output = <tweetId, tweetVect	\t tweetDesc>
				// same as mapper received
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
	        conf.set("hdfsPath", args[3]);
	        Job job = Job.getInstance(conf, "Create Tweet Vector Table");
	        job.setJar("createTweetVectorTable.jar");
	        job.setMapperClass(CreateTweetVectorTableMapper.class);
	        job.setMapOutputKeyClass(IntWritable.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setReducerClass(CreateTweetVectorTableReducer.class);
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	        job.setOutputKeyClass(LongWritable.class);
	        job.setOutputValueClass(Text.class);
	        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
	        SequenceFileOutputFormat.setOutputPath(job, new Path(args[2]));
	        Boolean success = job.waitForCompletion(true);
	        if(success){
	        	System.out.println("CreateTweetVectorTable: finished successfully");
	        	System.exit(0);
	        } else {
	        	System.out.println("CreateTweetVectorTable: failed");
	        	System.exit(1);
	        }
	 }

}


