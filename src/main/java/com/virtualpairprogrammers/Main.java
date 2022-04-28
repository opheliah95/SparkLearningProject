package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<Double> inputData = new ArrayList<Double>();
		inputData.add(3.5);
		inputData.add(12.345);
		inputData.add(50.32);
		inputData.add(20.345);
		
		// configure logging so there is not too much red
		/// remove unnecessary log from apache
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		// use spark in local config without a cluster -> run on all threads
		SparkConf conf = new SparkConf().setAppName("starting spark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// load data into spark -> a scalar wrapup
		JavaRDD<Double> myRdd = sc.parallelize(inputData);
		
		for (Double var: myRdd.collect()){
			System.out.println("*" + var);
		}
		sc.close();

	}

}
