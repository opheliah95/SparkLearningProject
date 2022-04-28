package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Tuple {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");
		
		
		// configure logging so there is not too much red
		/// remove unnecessary log from apache
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		// use spark in local config without a cluster -> run on all threads
		SparkConf conf = new SparkConf().setAppName("starting spark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// load data into spark -> a scalar wrapup
		JavaRDD<String> originalMessages = sc.parallelize(inputData);
		
		// create a pair RDD
		JavaPairRDD<String, String> pairRDD = originalMessages.mapToPair(rawValue -> {
			String[] columns = rawValue.split(":");
			String level = columns[0];
			String message = columns[1];
			return new Tuple2<String, String>(level, message);
		});
		
		// do something with string
		JavaPairRDD<String, Long> counterRDD = originalMessages.mapToPair(rawValue -> {
			String[] columns = rawValue.split(":");
			String level = columns[0];
			return new Tuple2<String, Long>(level, 1L);
		});
		
		JavaPairRDD<String, Long> keyCount = counterRDD.reduceByKey((a, b) -> a + b);
		
		keyCount.foreach(val -> System.out.println(val._1 + ":" + val._2));
		sc.close();

	}

}
