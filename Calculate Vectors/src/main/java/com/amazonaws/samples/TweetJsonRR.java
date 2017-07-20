package com.amazonaws.samples;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.JSONObject;

public class TweetJsonRR extends RecordReader<TweetKey, TweetValue> {
	
	LineRecordReader reader;
	
	public TweetJsonRR(){
		reader = new LineRecordReader(); 
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		reader.initialize(split, context);
	}

	@Override
	public void close() throws IOException {
		reader.close();
		
	}

	@Override
	public TweetKey getCurrentKey() throws IOException, InterruptedException {
		Text line = reader.getCurrentValue();
		JSONObject jsonObj = new JSONObject(line.toString());
		return new TweetKey(jsonObj.getString("created_at"), jsonObj.getLong("id"));
	}

	@Override
	public TweetValue getCurrentValue() throws IOException, InterruptedException {
		Text line = reader.getCurrentValue();
		JSONObject jsonObj = new JSONObject(line.toString());
	    return new TweetValue(jsonObj.getJSONObject("user").getString("name"),
	    					  jsonObj.getString("text"),
	    					  jsonObj.getBoolean("favorited"),
	    					  jsonObj.getBoolean("retweeted"));
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		reader.getProgress();
		return 0;
	}

	

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		 return reader.nextKeyValue();
	}
		
}
