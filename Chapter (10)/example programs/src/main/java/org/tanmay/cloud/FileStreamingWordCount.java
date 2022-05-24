package org.tanmay.cloud;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class FileStreamingWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: FileStreamingWordCount <data directory>");
            System.exit(1);
        }

        //Create a local streaming context with two threads and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("FileStreamingWordCount");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaDStream<String> lines = jsc.textFileStream(args[0]);
        

    }
}

