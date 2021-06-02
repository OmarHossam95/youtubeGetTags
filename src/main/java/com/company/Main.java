package com.company;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Main {
    private static final String COMMA_DELIMITER = ",";
    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("tagCount").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> videos = sparkContext.textFile("src/main/resources/USvideos.csv");

        JavaRDD<String> tags = videos.map(Main::extractTag);
        tags = tags.flatMap(tag -> Arrays.asList(tag.split("\\|")).iterator());

        Map<String, Long> tagCounts = tags.countByValue();
        List<Map.Entry> sorted = tagCounts.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());

        for (Map.Entry<String, Long> entry: sorted){
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    public static String extractTag(String video){
        try{
            return video.split(",")[6];
        }
        catch (ArrayIndexOutOfBoundsException e){
            return "";
        }
    }
}
