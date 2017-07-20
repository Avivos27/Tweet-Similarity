package com.amazonaws.samples;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class TweetValue implements WritableComparable<TweetValue>{
	private String userName;
	private String text;
	private Boolean fav;
	private Boolean retweeted;
	
	public TweetValue(String name,String t,Boolean f, Boolean retweeted){
		userName = name;
		text = t;
		fav = f;
		this.retweeted = retweeted;
		//System.out.println("Tweet value constructed "+" "+name+" "+text+" "+fav+" "+this.retweeted);
	}
	
	public String getUserName() {
		return userName;
	}
	
	public String getText() {
		return text;
	}
	
	public Boolean getFav() {
		return fav;
	}
	
	public Boolean getRetweeted() {
		return retweeted;
	}

	public void readFields(DataInput in) throws IOException {
		userName = in.readUTF();
		text = in.readUTF();
		fav = in.readBoolean();
		retweeted = in.readBoolean();
		
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(userName);
		out.writeUTF(text);
		out.writeBoolean(fav);
		out.writeBoolean(retweeted);
		
	}

	public int compareTo(TweetValue o) {
		
		return (userName+text).compareTo(o.userName+o.text);
	}
}
