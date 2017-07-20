package com.amazonaws.countWords;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
public class CountWordsInputFormat extends FileInputFormat<TweetKey, TweetValue> {
 
 
      @Override
      public RecordReader<TweetKey, TweetValue> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new TweetJsonRR();
      }
      
      @Override
      protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
          new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
      }
 
}