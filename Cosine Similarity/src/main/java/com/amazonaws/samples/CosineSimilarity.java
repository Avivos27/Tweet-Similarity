package com.amazonaws.samples;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CosineSimilarity {
	/*	=======================================	
	 * 	===============  Mapper ===============
	 *  ======================================= */
	public static class CosineSimilarityMapper extends Mapper<LongWritable, Text, Text, Text>{

	    @Override
	    // tweetVector = < index-0:tfidf-0, ... , index-n:tfidf-n>
	    // tweetDesc = text \t createdAt \t fav \t retweeted
	    public void map(LongWritable tweetId, Text vectorDesc, Context context) throws IOException,  InterruptedException {
	    
	    	HashMap<Integer, Double> vectorMap= new HashMap<Integer, Double>();
			double A = 0;
			double B = 0;
			double AB = 0;
			double normA = 0;
			double normB = 0;
			int counter=0;
			int minIndex = 0;
			Configuration conf = context.getConfiguration();
			int N = Integer.parseInt(conf.get("N"));
			CosineTextPair TopN[] = new CosineTextPair[N];
			
			String myVectorStr = vectorDesc.toString().split("\t")[0];
			String vectorEntrys[] = myVectorStr.split(",");
			for(int i=0;i<vectorEntrys.length;i++){
				String entry[] = vectorEntrys[i].split(":");
				A+=Math.pow(Double.parseDouble(entry[1]), 2);
				vectorMap.put(Integer.parseInt(entry[0]), Double.parseDouble(entry[1]));	
			}
			normA = Math.sqrt(A);
			
			
			String tweetDesc = vectorDesc.toString().split("\t",2)[1];
			String tweetDescArr[] = tweetDesc.split("\t");
			TweetContent myTweet = new TweetContent(tweetId.get(), //id
													tweetDescArr[1], //createdAt
													Boolean.parseBoolean(tweetDescArr[2]), //fav
													Boolean.parseBoolean(tweetDescArr[3]), //retweeted
													tweetDescArr[0]); // text
	    	

	    	conf = context.getConfiguration();
	 		String path = conf.get("tweetVectorTablePath");
	 		Path pt=new Path(path);
	 		FileSystem fs = FileSystem.get(context.getConfiguration());
	 		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	 		try {
	 		  String tweetIdVectorAndTextLine;
	 		  tweetIdVectorAndTextLine=br.readLine();
	 		  while (tweetIdVectorAndTextLine != null){
	 			
	 			String tweetArr[] = tweetIdVectorAndTextLine.split("\t");
	 			long currTweetId = Long.parseLong(tweetArr[0]);
	 			if(myTweet.getId() != currTweetId){
		 				
		 			boolean foundSimilarity = false;
		 			String currVectorStr = tweetArr[1];
					String currVectorEntries[] = currVectorStr.split(",");
					B = 0;
					AB = 0;
					
					// dot product between vectors, inform if found similarity
					for(int i=0;i<currVectorEntries.length;i++){
						String entry[] = currVectorEntries[i].split(":");
						Integer index = Integer.parseInt(entry[0]);
						Double tf_idf = Double.parseDouble(entry[1]);
						B += Math.pow(tf_idf, 2);
						if(vectorMap.containsKey(index)){
							AB = AB + (vectorMap.get(index).doubleValue()*tf_idf.doubleValue());
							foundSimilarity = true;
						}
					}
					normB = Math.sqrt(B);
					
					if(foundSimilarity){
						double cosineSim = AB/(normA*normB);
						
						// insert to topN if necessary 
						if (counter<N){
							CosineTextPair currEntry = new CosineTextPair(tweetArr[2], cosineSim);
							TopN[counter] = currEntry;
							for(int i=0; i<=counter; i++){
								if(TopN[i].getCosine() < TopN[minIndex].getCosine()){
									minIndex = i;
								}
							}
							counter++;
						}
						else{
							if(cosineSim > TopN[minIndex].getCosine()){
								CosineTextPair currEntry = new CosineTextPair(tweetArr[2], cosineSim);
								TopN[minIndex] = currEntry;
								for (int i=0; i<TopN.length; i++){
									if(TopN[i].getCosine() < TopN[minIndex].getCosine()){
										minIndex =  i;
									}
								}
							}
						}
					}
	 			}
	 			 tweetIdVectorAndTextLine = br.readLine();
	 		  }
	 		} finally {
	 		  br.close();
	 		}
	 		
	 		
	 		
	 		counter = Math.min(counter, TopN.length)-1;
			Arrays.sort(TopN, 0, counter+1);
			String toWrite = "\n";
			
			for(int i=counter; i>=0; i--){
				toWrite += "\t" + TopN[i].getCosine() + "\t" +TopN[i].getText() + "\n";
			}
			context.write(myTweet.toText(), new Text(toWrite));
			// emit final output - tweetId and topN similarities
	    }
	}
	
	 /*	 =======================================	
	  *  =============== Main ==================
	  *  ======================================= */
	 public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	        conf.set("tweetVectorTablePath", args[3]);
	        conf.set("N", args[4]);
	        Job job = Job.getInstance(conf, "Cosine Similarity");
	        job.setJar("cosineSimilarity.jar");
	        job.setMapperClass(CosineSimilarityMapper.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setNumReduceTasks(0);
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
	        TextOutputFormat.setOutputPath(job, new Path(args[2]));
	        Boolean success = job.waitForCompletion(true);
	        if(success){
	        	System.out.println("Cosine: finished successfully");
	        	System.exit(0);
	        } else {
	        	System.out.println("Cosine : failed");
	        	System.exit(1);
	        }
	 }

}
