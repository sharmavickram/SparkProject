package com.dataedge.wordcount;

import static org.junit.Assert.assertEquals;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

public class WordCountTest1 {

	@Test
	public void testCount() {
		JavaPairRDD<String, Integer> listSize = new WordCount1().wordCount();
		
		assertEquals("was expecting %s but found %s", 15213, listSize.collect().size());

	}

}
