package com.dataedge.wordcount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
/**
 * @author ShaRmaVikRam
 * @DataedgeSystems.online
 */
public class WordCount {

	private static final Pattern SPACE = Pattern.compile(" ");

	public int wordCountJob() {
		
		SparkSession spark = SparkSession.builder().appName("wordCount").master("local[*]").getOrCreate();
		
		String path = "R:\\hadoop\\resources\\txt\\holmes.txt";
		List<Object> list = new ArrayList<>();
		JavaRDD<String> lines = spark.read().textFile(path).javaRDD();
		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			list.add(tuple._1());
		}
		spark.stop();
		return list.size();
	}
}