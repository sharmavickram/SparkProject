package com.dataedge.wordcount;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class WordCountTest {

	@Test
	public void testwordCountJob() {
		int listSize = new WordCount().wordCountJob();
		assertEquals("was expecting %s but found %s",15213, listSize);
	}
}