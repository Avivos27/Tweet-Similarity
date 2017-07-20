package com.amazonaws.samples;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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

public class CalculateVector {
	
	/*	=======================================	
	 * 	===============  Mapper ===============
	 *  ======================================= */
	public static class CalculateVectorMapper extends Mapper<TweetKey, TweetValue, LongWritable, Text>{
		
	    private Text word = new Text();
	    private HashMap<Text, Boolean> stopWords;
	    
	    @Override
        public void setup(Context context) throws IOException, InterruptedException {
	    	stopWords = new HashMap<Text,Boolean>();
	    	
	    	@SuppressWarnings("deprecation")
			AmazonS3 s3 = new AmazonS3Client();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);
            S3Object object = s3.getObject(new GetObjectRequest("tweet-similarity/args", "stop_words.txt"));
            
            // initialize stopWords hashMap
            Scanner sc = new Scanner(new InputStreamReader(object.getObjectContent()));
            while (sc.hasNextLine()) {
                String word = sc.nextLine();
                stopWords.put(new Text(word), true);
            }
            sc.close();
            
        }
	    
	    
	    @Override
	    public void map(TweetKey key, TweetValue value, Context context) throws IOException,  InterruptedException {
	      LongWritable tweetId = key.getId();
	      
	      StringTokenizer itr = new StringTokenizer(value.getText()); 
	      while (itr.hasMoreTokens()) {
	    	// parse word to lowerCase and remove whitespaces and newlines
	        word.set(itr.nextToken());
	        word.set(word.toString().toLowerCase().replace("\t", "").replace("\r", "").replace("\n", ""));
	        if(word.toString().compareTo("") != 0 && !stopWords.containsKey(word)){
	        	// emit for each word in tweet text
	        	context.write(tweetId, word);
	        }
	      }
	      
	      Text selfTweetDesc = new Text(value.getText() + "\t" +
	    		  		key.getCreatedAt() + "\t" + 
	    		  		value.getFav() + "\t" + 
	    		  		value.getRetweeted());
	      // emit once only tweet details	  	   
	      context.write(tweetId, selfTweetDesc);
	    }
	}

	
	/*	=======================================	
	 * 	============== Reducer ================
	 *  ======================================= */
	 public static class CalculateVectorReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		 	HashMap<Text,Integer[]> dictionary;
		 	static Text tweetIndicator = new Text("^^&&***TOTALNUMBEROFTWEETS***&&^^");
		 	
		 	
		 	@Override
		 	public void setup(Context context) throws IOException, InterruptedException {
		 		dictionary = new HashMap<Text, Integer[]>();
		 		
		 		// read one line at a time from hdfs dictionary and save it in memory as hashMap
		 		Configuration conf = context.getConfiguration();
		 		String path = conf.get("dictionaryPath");
		 		Path pt=new Path(path);
		 		FileSystem fs = FileSystem.get(context.getConfiguration());
		 		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		 		try {
		 		  String dictLine;
		 		  dictLine=br.readLine();
		 		  while (dictLine != null){
		 			 String wordTupArr[] = dictLine.split("\t"); // split by tab
		             String word = wordTupArr[0];
		             String index = wordTupArr[1]; 
		             String tweetCount = wordTupArr[2];
		             Integer tup[] = {Integer.parseInt(index), Integer.parseInt(tweetCount)};
		             
		             // dictionary entry: <word: {vector index, tweet count}>
		             dictionary.put(new Text(word),tup);

		 			 dictLine = br.readLine();
		 		  }
		 		} finally {
		 		  br.close();
		 		}
		 	}
		 	
		 	
		    @Override
		    public void reduce(LongWritable tweetId, Iterable<Text> wordsInTweet, Context context) throws IOException,  InterruptedException {
		    	HashMap<Text, Integer> tweetWord = new HashMap<Text, Integer>();
		    	int max = 0;
		    	Double N = new Double(dictionary.get(tweetIndicator)[1]);
		    	Text tweetDesc = null;
		    	String vectorStr="";
		    	
		    	for(Text wordIter: wordsInTweet){ 
		    		//for each word under tweet with tweet id = key
		    		Text word = new Text(wordIter);
		    		
		    		String selfDesc[] = word.toString().split("\t");
		    		//only self tweetDesc is \t delimited
		    		if(selfDesc.length > 1){ 
		    			tweetDesc = word;
		    		}
		    		else{
			    		if(!tweetWord.containsKey(word)){
			    			tweetWord.put(word, new Integer(1)); 
			    			if(max<1){
			    				max=1;
			    			}
			    		}
			    		else{
		    				int count = tweetWord.get(word)+1;
			    			tweetWord.put(word, count);
			    			if(max<count){
			    				max=count;
			    			}
			    		}
		    		}
		    	}
		    	
		    	// iterate over tweet words map to calculate tf-idf for each word
		    	for (Map.Entry<Text, Integer> entry : tweetWord.entrySet()){
		    		Text word = entry.getKey();
					Integer wordCount = entry.getValue();
		    		
		    		double tf = 0.5+0.5*(wordCount/max);
		    		
		    		Integer arr[] = dictionary.get(word);
		    		if(arr != null){
		    			IntWritable wordIndex =new IntWritable(arr[0]);
			    		Double tweetCount = new Double(arr[1]);
			    		double idf = Math.log(N/tweetCount);		
			    		DoubleWritable tf_idf = new DoubleWritable(tf*idf);
			    		
			    		vectorStr += wordIndex.toString()+":"+tf_idf.toString()+",";
		    		}
		    	}
		    	vectorStr = vectorStr.substring(0,vectorStr.length()-1); // trim last comma
		    	
		    	context.write(tweetId, new Text(vectorStr + "\t" + tweetDesc));
		    	// output = <tweetId, tweetVector \t tweetDesc >
		    	// where tweetDesc = text \t createdAt \t fav \t retweeted
		    }
	 }
	 
	 
	 /*	 =======================================	
	  *  =============== Main ==================
	  *  ======================================= */
	 public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("dictionaryPath", args[3]);
        
        Job job = Job.getInstance(conf, "Count Words");
        job.setJar("calcVectors.jar");
        job.setMapperClass(CalculateVectorMapper.class);
        job.setReducerClass(CalculateVectorReducer.class);
        job.setInputFormatClass(CountWordsInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
        CountWordsInputFormat.addInputPath(job, new Path(args[1]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[2])); 
        
        Boolean success = job.waitForCompletion(true);
        if(success){
        	System.out.println("CalculateVector: finished successfully");
        	System.exit(0);
        } else {
        	System.out.println("CalculateVector: failed");
        	System.exit(1);
        }
	 }
	 
}