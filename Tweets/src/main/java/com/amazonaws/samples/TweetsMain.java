package com.amazonaws.samples;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

public class TweetsMain {

	public static void main(String[] args) {
		String N;
		if(args.length > 0){
			N = args[0];
			System.out.println("N is : "+N);
		}
		else{
			System.out.println("N is default 10");
			N="10";
		}
		
		System.out.println("Running tweetsMain...");
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
		
		
		@SuppressWarnings("deprecation")
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
		
//		String testCorpus = "s3n://tweet-similarity/input-corpus/test.txt";
//		String tinyCorpus = "s3n://tweet-similarity/input-corpus/TinyCorpus.txt";
//		String corpus200 = "s3n://tweet-similarity/input-corpus/200.txt";
//		String corpus10000 = "s3n://tweet-similarity/input-corpus/10000.txt";
//		String corpus5000 = "s3n://tweet-similarity/input-corpus/5000.txt";
		String smallCorpus = "s3n://tweet-similarity/input-corpus/smallCorpus.txt";
		System.out.println("Successfully authenticated using credentials");
		UUID random = UUID.randomUUID();
		System.out.println("Output folder is output-count-words/"+random);
		HadoopJarStepConfig countWordsJarStep = new HadoopJarStepConfig()
		    .withJar("s3n://tweet-similarity/jars/countWords-ex.jar") 
		    .withMainClass("CountWords")
		    .withArgs(smallCorpus,"hdfs:///output-countWords/");
        
        
		StepConfig stepConfigCountWords = new StepConfig()
		    .withName("Count Words")
		    .withHadoopJarStep(countWordsJarStep)
		    .withActionOnFailure("TERMINATE_JOB_FLOW");
		

		HadoopJarStepConfig createDictJarStep = new HadoopJarStepConfig()
			    .withJar("s3n://tweet-similarity/jars/createDict.jar") 
			    .withMainClass("CreateDict")
			    .withArgs("hdfs:///output-countWords/","hdfs:///output-dictionary/", "hdfs:///output-dictionaryHardCoded/");
			 
		StepConfig stepConfigCreateDict = new StepConfig()
			    .withName("Create Dict")
			    .withHadoopJarStep(createDictJarStep)
			    .withActionOnFailure("TERMINATE_JOB_FLOW");
		
		HadoopJarStepConfig calculateVectorJarStep = new HadoopJarStepConfig()
			    .withJar("s3n://tweet-similarity/jars/calcVectors.jar") 
			    .withMainClass("CalculateVector")
			    .withArgs(smallCorpus,"hdfs:///output-calc-vector/","hdfs:///output-dictionaryHardCoded/");

		StepConfig stepConfigCalculateVector = new StepConfig()
			    .withName("Calculate Vector")
			    .withHadoopJarStep(calculateVectorJarStep)
			    .withActionOnFailure("TERMINATE_JOB_FLOW");
		
		HadoopJarStepConfig createTweetVectorTableJarStep = new HadoopJarStepConfig()
			    .withJar("s3n://tweet-similarity/jars/createTweetVectorTable.jar") 
			    .withMainClass("CreateTweetVectorTable")
			    .withArgs("hdfs:///output-calc-vector/","hdfs:///output-create-tweet-vector-table/","hdfs:///tweetVectorTable/");
			 
		StepConfig stepConfigCreateTweetVectorTable = new StepConfig()
			    .withName("Create Tweet Vector Table")
			    .withHadoopJarStep(createTweetVectorTableJarStep)
			    .withActionOnFailure("TERMINATE_JOB_FLOW");
		
		HadoopJarStepConfig CosineSimilarityJarStep = new HadoopJarStepConfig()
			    .withJar("s3n://tweet-similarity/jars/cosineSimilarity.jar") 
			    .withMainClass("CosineSimilarity")
			    .withArgs("hdfs:///output-create-tweet-vector-table/","s3n://tweet-similarity/output/"+random,"hdfs:///tweetVectorTable/",N);
			 
		StepConfig stepConfigCosineSimilarity = new StepConfig()
			    .withName("Cosine Similarity")
			    .withHadoopJarStep(CosineSimilarityJarStep)
			    .withActionOnFailure("TERMINATE_JOB_FLOW");
				
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		    .withInstanceCount(5)
		    .withMasterInstanceType(InstanceType.M1Large.toString())    
		    .withSlaveInstanceType(InstanceType.M1Large.toString())	 
		    .withHadoopVersion("2.2.0").withEc2KeyName("emr")		    
		    .withKeepJobFlowAliveWhenNoSteps(false)
		    .withPlacement(new PlacementType("us-east-1b"));
		
		System.out.println("Successfully configured instances");
		 
		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		 //   .withName("Tweets_EMR")
			.withName("smallCorpus")
		    .withInstances(instances)
		    .withSteps(stepConfigCountWords, stepConfigCreateDict , stepConfigCalculateVector
		    		   ,stepConfigCreateTweetVectorTable, stepConfigCosineSimilarity)
		    .withLogUri("s3n://tweet-similarity/logs/");
		
		runFlowRequest.setServiceRole("emr");
		runFlowRequest.setJobFlowRole("ec2_emr");
		
		System.out.println("Successfully configured runFlowRequest");
		
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		System.out.println("Submiting job flow");
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId + "output folder: " + random);

	}

}
