package com.RUSpark;

/* any necessary Java packages here */

import scala.Tuple2;
//import scala.Tuple4;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class RedditPhotoImpact {

	//private static final Patter SEP = Pattern.compile(",");

	public static void main(String[] args) throws Exception {

    		if (args.length < 1) {
      			System.err.println("Usage: RedditPhotoImpact <file>");
      			System.exit(1);
    		}
		
		String InputPath = args[0];
		
		/* Implement Here */
		SparkSession spark = SparkSession
			.builder()
			.appName("RedditPhotoImpact")
			.getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

		//JavaRDD<Array<String>> elements = lines.map(l -> l.split(","));

		JavaPairRDD<String, Integer> instanceImpact = lines.mapToPair(l -> new Tuple2<>(l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[0], 
					
					Integer.parseInt(l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[4].strip())+
					Integer.parseInt(l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[5].strip())+
					Integer.parseInt(l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[6].strip()))); 


		JavaPairRDD<String, Integer> photoImpact = instanceImpact.reduceByKey((i1, i2) -> i1 + i2);

		JavaPairRDD<String, Integer> photoImpact_sorted = photoImpact.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());

		List<Tuple2<String, Integer>> output = photoImpact_sorted.collect();
    		for (Tuple2<?,?> tuple : output) {
      			System.out.println(tuple._1() + " " + tuple._2());
    		}

    		spark.stop();
		
	}

}
