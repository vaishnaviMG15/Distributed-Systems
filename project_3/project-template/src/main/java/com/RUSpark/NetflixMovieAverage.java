package com.RUSpark;

/* any necessary Java packages here */
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
//import java.text.DecimalFormat;

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

    		if (args.length < 1) {
      			System.err.println("Usage: NetflixMovieAverage <file>");
      			System.exit(1);
    		}
		
		String InputPath = args[0];
		
		/* Implement Here */

		SparkSession spark = SparkSession
                        .builder()
                        .appName("NetflixMovieAverage")
                        .getOrCreate();


                JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

		//Key: movie id; Value: single rating for movie
                JavaPairRDD<String, Integer> rating = lines.mapToPair(l -> new Tuple2<>(l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[0].strip(),
									Integer.parseInt(l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[2].strip())));
		
		//Key: movie id; Value: sum of all ratings for the movie
		JavaPairRDD<String, Integer> totalRating = rating.reduceByKey((i1, i2) -> i1 + i2);

		//Key: movie id; Value: 1
		JavaPairRDD<String, Integer> counter = lines.mapToPair(l -> new Tuple2<>(l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)[0].strip(), 1));

		//Key: movie id; Value: total number of ratings for this movie
		JavaPairRDD<String, Integer> counts = counter.reduceByKey((i1, i2) -> i1 + i2);
		
		//Basically for a movie (Key), avgRating = value of this key in totalRating / value of this key in counts
		//JavaPairRDD<String, Double> avgRating = (totalRating.join(counts)).mapValues(lambda x: ((x[0] * 1.0) / x[1]));
		//JavaPairRDD<String, Double> avgRating = (totalRating.join(counts)).mapValues(x -> ((x[0] * 1.0) / x[1]));
		JavaPairRDD<String, Double> avgRating = (totalRating.join(counts)).mapValues(x -> ((x._1() * 1.0) / x._2()));
	
		JavaPairRDD<String, Double> avgRating_sorted = avgRating.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());	
		List<Tuple2<String, Double>> output = avgRating_sorted.collect();
    		for (Tuple2<?,?> tuple : output) {
      			//System.out.println(tuple._1() + " " + tuple._2());
    			System.out.println(tuple._1() + " " + String.format("%.2f",tuple._2()));
			//System.out.println(tuple._1() + " " + new DecimalFormat("#.##").format(((Double)tuple._2()).doubleValue()));
		}

    		spark.stop();


	}

}
