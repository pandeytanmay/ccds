package org.tanmay.cloud;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleRdd {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage : SimpleRdd <Spark Url>");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName("SimpleRdd").setMaster(args[0]);
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Create and RDD with a simple list of integers
        List<Integer> data = IntStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList());
        JavaRDD<Integer> distData = sc.parallelize(data);
        //Don't do this if your RDD contains a lot of records
        distData.collect().forEach(x -> System.out.println(x));
        JavaRDD<Integer> mappedData = distData.map((a) -> 2*a);
        //Prints 1 to 1000
        distData.collect().forEach(x -> System.out.println(x));
        //Prints all even numbers upto 2000
        mappedData.collect().forEach(x -> System.out.println(x));
        long sum = distData.reduce((a, b) -> a + b);
        System.out.println("Sum : " + sum);
    }
}
