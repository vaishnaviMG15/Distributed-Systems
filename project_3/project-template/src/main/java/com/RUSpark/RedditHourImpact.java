package com.RUSpark;

/* any necessary Java packages here */
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import java.util.*;
import java.text.*;

public class RedditHourImpact {
	/*
	private static Tuple2<String, Integer> convertToESTH (Tuple2<String, Integer> input){

		SimpleDateFormat d_f = new SimpleDateFormat("H");
                d_f.setTimeZone(TimeZone.getTimeZone("America/New_York"));
		Date date = new Date(input.getValue1());
		String s_hour = d_f.format(date);
		int hour = Integer.parseInt(s_hour);
		//Tuple2<String, Integer> result = [input.getValue0(),hour];
		return [input.getValue0(),hour];

	}
	*/

	public static void main(String[] args) throws Exception {

    		if (args.length < 1) {
      			System.err.println("Usage: RedditHourImpact <file>");
      			System.exit(1);
    		}
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
                        .builder()
                        .appName("RedditHourImpact")
                        .getOrCreate();

		SimpleDateFormat d_f = new SimpleDateFormat("H");
		d_f.setTimeZone(TimeZone.getTimeZone("America/New_York"));

                JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
		
		
		JavaPairRDD<Integer, Integer> est_hours = lines.mapToPair(l -> new Tuple2<>(
                                                      Integer.parseInt(d_f.format(new Date(Long.parseLong(l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[1].strip()) * 1000L))),
                                                      Integer.parseInt(l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[4].strip())+
                                        		Integer.parseInt(l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[5].strip())+
                                        		Integer.parseInt(l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[6].strip())
						      ));
		
		
		
		JavaPairRDD<Integer, Integer> hour_impact = est_hours.reduceByKey((i1, i2) -> i1 + i2);	
		JavaPairRDD<Integer, Integer> hour_impact_sorted = hour_impact.sortByKey();
		

		List<Tuple2<Integer, Integer>> output = hour_impact_sorted.collect();

		int i = 0;

		for (Tuple2<?,?> tuple : output) {
			for(;i<((Integer)tuple._1()).intValue();i++){

				System.out.println(i + " 0");

			}
      			System.out.println(tuple._1() + " " + tuple._2());
			i++;
    		}


		for(;i<24;i++){

			System.out.println(i + " 0");

		}

		
    		spark.stop();

	}

}
