package com.RUSpark;

/* any necessary Java packages here */
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
//import java.util.Collections;
import java.util.ArrayList;

import java.util.*;

public class NetflixGraphGenerate {
	
	private static ArrayList<Tuple2<Integer, Integer>> toPairs(Iterable<Integer> input){
		
		ArrayList<Integer> in = new ArrayList<Integer>();

		//Iterable<Integer> iter = input.iterator();

		for (int i: input){
			in.add(i);
		}		

		ArrayList<Tuple2<Integer, Integer>> output = new ArrayList<Tuple2<Integer, Integer>>();
		
		for(int i = 0; i < in.size(); i++){
                        
                        //form a pair of this element and all consecutive elements
                        for(int j = i+1; j < in.size(); j++){
                                
                                //form the pair (element at ith location of input_list, element at jth location of input_list)
                                Tuple2<Integer, Integer> element = new Tuple2<>(in.get(i), in.get(j));
                                output.add(element);

                        }

                }	

		return output;

	}
	
	private static Tuple2<Integer, Integer> sortTuple (Tuple2<Integer, Integer> input){


		if(input._1() < input._2()){

			return input;

		}else{

			return (new Tuple2<>(input._2(), input._1()));

		}

	}

	public static void main(String[] args) throws Exception {

    		if (args.length < 1) {
     			System.err.println("Usage: NetflixGraphGenerate <file>");
      			System.exit(1);
    		}
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
      				.builder()
      				.appName("NetflixGraphGenerate")
      				.getOrCreate();

    		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

		//Key: (movie, rating); Value: customer_id
		JavaPairRDD<Tuple2<String, String>,Integer> rating = lines.mapToPair(l -> new Tuple2<>(new Tuple2<>(l.split(",")[0].strip(), l.split(",")[2].strip()), 
														Integer.parseInt(l.split(",")[1].strip())));
		
		//Key: (movie, rating); Value: customer list
		JavaPairRDD<Tuple2<String, String>,Iterable<Integer>> commonCustomers = rating.groupByKey();
		

		//each RDD entry is a list of common customers
		JavaRDD<Iterable<Integer>> commonCustomersLines = commonCustomers.map(e -> e._2());
	
		//each RDD is a pair (customer A, customer B)
		//(s -> Arrays.asList(array).iterator());
		JavaRDD<Tuple2<Integer, Integer>> connections = commonCustomersLines.flatMap(e -> (toPairs(e)).iterator());	

		//each RDD is a pair (customer A, cutomer B) where customer A < customer B
		JavaRDD<Tuple2<Integer, Integer>> sorted_connections = connections.map(e -> sortTuple(e));	

		//key: (customer A, cutomer B) where customer A < customer B; value: 1
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> conn_count = sorted_connections.mapToPair(e -> new Tuple2<>(e, 1)); 

		//key: (customer A, cutomer B) where customer A < customer B; value: # of connections btw customers A and B
                JavaPairRDD<Tuple2<Integer, Integer>,Integer> conn_total = conn_count.reduceByKey((i1, i2) -> i1 + i2);
		
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> conn_total_sorted = conn_total.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());

		List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = conn_total_sorted.collect();
    		for (Tuple2<?,?> tuple : output) {

			Tuple2<Integer, Integer> tupleA = (Tuple2<Integer, Integer>)tuple._1();
			int a1 = tupleA._1(); 
			int a2 = tupleA._2();
			int b = ((Integer)tuple._2()).intValue();
      			System.out.println("(" + a1 + "," + a2 + ")" + " " + b);
    		}
		
    		spark.stop();	
	
	}

}
