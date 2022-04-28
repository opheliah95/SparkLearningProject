package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class Tuple {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<Integer> inputData = new ArrayList<Integer>();
		inputData.add(3);
		inputData.add(12);
		inputData.add(50);
		inputData.add(20);
		
		// configure logging so there is not too much red
		/// remove unnecessary log from apache
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		// use spark in local config without a cluster -> run on all threads
		SparkConf conf = new SparkConf().setAppName("starting spark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// load data into spark -> a scalar wrapup
		JavaRDD<Integer> myRdd = sc.parallelize(inputData);
		
		// reduces rdd
		JavaRDD<Double> sqrtRdd= myRdd.map(var -> Math.sqrt(var) );

		sqrtRdd.foreach(val -> System.out.println("the value is: " + val));
		
		Long count = sqrtRdd.count();
		System.out.println("total data in the set: " + count);
		
		// more spark native way to do the counting
		JavaRDD<Integer> r = sqrtRdd.map(val -> 1);
		Integer c = r.reduce((val1, val2) -> val1 + val2);
		System.out.println("total count of interger is " + c);
		sc.close();

	}

}
