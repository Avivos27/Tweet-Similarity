package com.amazonaws.countWords;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class TweetKey implements WritableComparable<TweetKey>{
	private String createdAt;
	private long id;
	
	public TweetKey(String createAt, long id){
		this.createdAt = createAt;
		this.id = id;
	}
	
	public String getCreatedAt() {
		return createdAt;
	}
	
	public LongWritable getId() {
		return new LongWritable(id);
	}
	public void readFields(DataInput in) throws IOException {
		createdAt = in.readUTF();
		id = in.readInt();
		
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(createdAt);
		out.writeLong(id);
		
	}
	public int compareTo(TweetKey other) {
		if((id - other.getId().get()) == 0){
			return createdAt.compareTo(other.getCreatedAt());
		}
		else{
			return (int) (id - other.getId().get());
		}
	}
}
