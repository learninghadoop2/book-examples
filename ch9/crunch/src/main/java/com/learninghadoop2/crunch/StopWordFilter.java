package com.learninghadoop2.crunch;

import org.apache.crunch.FilterFn;

import com.google.common.collect.ImmutableSet;


public class StopWordFilter extends FilterFn<String>{

	// English stopwords from StopAnalyzer.ENGLISH_STOP_WORDS_SET in lucene
	 ImmutableSet<String> stopWords = ImmutableSet.copyOf(new String[] {
		      "a", "and", "are", "as", "at", "be", "but", "by",
		      "for", "if", "in", "into", "is", "it",
		      "no", "not", "of", "on", "or", "s", "such",
		      "t", "that", "the", "their", "then", "there", "these",
		      "they", "this", "to", "was", "will", "with"
		  });
	 
	 @Override
	 public boolean accept(String word) {
		 return !stopWords.contains(word);
	 }
}
