package com.amazonaws.countWords;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class CountWords {
	
				/*	=======================================	
				 * 	===============  Mapper ===============
				 *  ======================================= */
	public static class CountWordsMapper extends Mapper<TweetKey, TweetValue, Text, LongWritable>{
		
		private final static LongWritable one = new LongWritable(1);
	    private Text word = new Text();
	    private HashMap<Text, Boolean> stopWords = new HashMap<Text,Boolean>();
	    private Text tweetIndicator = new Text("^^&&***TOTALNUMBEROFTWEETS***&&^^");
	    
	    @Override
        public void setup(Context context) throws IOException, InterruptedException {
	    	
	    	// get stop-words from s3
	    	@SuppressWarnings("deprecation")
			AmazonS3 s3 = new AmazonS3Client();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);
            S3Object object = s3.getObject(new GetObjectRequest("tweet-similarity/args", "stop_words.txt"));            
            
            // setup stopWords hashMap
            Scanner sc = new Scanner(new InputStreamReader(object.getObjectContent()));
            while (sc.hasNextLine()) {
                String word = sc.nextLine();
                stopWords.put(new Text(word), true);
            }
            sc.close();
            
        }
	    
	    
	    @Override
	    public void map(TweetKey key, TweetValue value, Context context) throws IOException,  InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.getText());
	      HashMap<Text, Boolean> wordsMap = new HashMap<Text, Boolean>(); 
	      
	      while (itr.hasMoreTokens()){
	        word.set(itr.nextToken().toLowerCase());
	        
	        // do not emit stop words and duplicate words in specific tweet more than once
	        if(!stopWords.containsKey(word) && !wordsMap.containsKey(word)){ 
	        	wordsMap.put(word, true);
	        	context.write(word, key.getId()); 
	        	// emit <word, tweetId> for the reducer
	        	// reducer gets <word, listOftweetIds>
	        	//in order to count in how many different tweets the word appeared
	        }
	      }
	      
	      context.write(tweetIndicator,one); 
	      // send <tweetIndicator, 1> for the reducer once for each tweet to count the number of different tweets
	      

	    }
	}

	
				/*	=======================================	
				 * 	============== Reducer ================
				 *  ======================================= */
	 public static class CountWordsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		 	Text tweetIndicator = new Text("^^&&***TOTALNUMBEROFTWEETS***&&^^");
		 	
		    @Override
		    // reducer key is a word and values are list of tweets id`s where the word appeared in
		    // in case of special form tweetIndicator just increment counter to count number of tweets in corpus
		    public void reduce(Text wordKey, Iterable<LongWritable> tweetIds, Context context) throws IOException,  InterruptedException {

				long tweetCount = 0;				
				for(@SuppressWarnings("unused") LongWritable tweetId: tweetIds){
					tweetCount++;
				}

				context.write(wordKey,new LongWritable(tweetCount));
				// the reducer output is <word, number of tweets that the word appear in>
				// in case of special form tweetIndicator output is <tweetIndicator, total number of tweets in corpus>
		    }
		  }
	 
			 /*	 =======================================	
			  *  =============== Main ==================
			  *  ======================================= */
	 public static void main(String[] args) throws Exception {
		 
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        
        Job job = Job.getInstance(conf, "Count Words"); 
        job.setJar("countWords-ex.jar");
        job.setMapperClass(CountWords.CountWordsMapper.class);
        job.setReducerClass(CountWords.CountWordsReducer.class);       
        job.setInputFormatClass(CountWordsInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        CountWordsInputFormat.addInputPath(job, new Path(args[1]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        Boolean success = job.waitForCompletion(true);
        if(success){
        	System.out.println("CountWords: finished successfully");
        	System.exit(0);
        } else {
        	System.out.println("CountWords: failed");
        	System.exit(1);
        }
	 }
}
