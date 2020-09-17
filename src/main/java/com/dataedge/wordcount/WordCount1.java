package com.dataedge.wordcount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount1 {

	@SuppressWarnings("resource")
	public JavaPairRDD<String, Integer> wordCount() {
		SparkConf conf = new SparkConf().setAppName("WORD COUNT").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String path = "R:\\hadoop\\resources\\txt\\holmes.txt";
		JavaRDD<String> rdd = sc.textFile(path);
		System.out.println("TotalCount: " + rdd.count());

		JavaPairRDD<String, Integer> counts = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);

		return counts;
	}

}
