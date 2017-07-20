package com.amazonaws.samples;

public class CosineTextPair implements Comparable<CosineTextPair>{
	private String text;
	private double cosine;
	
	public CosineTextPair(String text, double cosine){
		this.text = text;
		this.cosine = cosine;
	}
	
	public double getCosine() {
		return cosine;
	}
	public String getText() {
		return text;
	}

	public int compareTo(CosineTextPair o) {
		double res = this.cosine - o.cosine;
		if(res > 0)
			return 1;
		else if(res < 0)
			return -1;
		else
			return 0;
	}

}
