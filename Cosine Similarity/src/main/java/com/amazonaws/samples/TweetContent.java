package com.amazonaws.samples;


import org.apache.hadoop.io.Text;


public class TweetContent implements Comparable<TweetContent>{
	private String text;
	private Boolean fav;
	private Boolean retweeted;
	private String createAt;
	private long id;
	private double cosine;
	
	
	public TweetContent(long id, String createdAt, boolean fav, boolean retweeted,String text){
		this.text = text;
		this.fav = fav;
		this.retweeted = retweeted;
		this.createAt = createdAt;
		this.id = id;
		cosine = 0;
	}
	
	public void setCosine(double cosine) {
		this.cosine = cosine;
	}

	public long getId() {
		return id;
	}
	public String getCreateAt() {
		return createAt;
	}
	public double getCosineSim() {
		return cosine;
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
	
	public Text toText(){
		return new Text("ID: "+ id +
						" Created at: "+ createAt + 
						" Favorited " + fav + 
						" Retweeted: "+ retweeted+ 
						" Text: "+ text);
	}
	
	public String getCosineText(){
		return "Cosine Similarity: " + cosine + " Text: " + text;
	}
	public int compareTo(TweetContent other) {
		double result =  this.cosine - other.cosine;
		if(result == 0)
			return 0;
		else if(result > 0)
			return 1;
		else 
			return -1;
	}
}
