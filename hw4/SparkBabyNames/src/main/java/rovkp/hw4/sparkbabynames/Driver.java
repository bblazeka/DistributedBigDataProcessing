/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw4.sparkbabynames;

import java.io.Serializable;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author bruno
 */
public class Driver {

    private static final String INPUT = "StateNames.csv";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BabyNames");

        //set the master if not already set through the command line
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local");
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        //create an RDD from text file lines
        JavaRDD<USBabyNameRecord> babyNames = sc.textFile(INPUT)
                .map(USBabyNameRecord::create)
                .filter(Objects::nonNull);

        switch (1) {
            case 1:
                // most unpopular female name
                String unpopularName = babyNames
                        .filter(e -> e.getGender().toLowerCase().equals("f"))
                        .mapToPair(n -> new Tuple2<>(n.getName(), n.getCount()))
                        .reduceByKey((x, y) -> x + y)
                        .min(COMPARATOR)._1;
                System.out.println("Most unpopular female name: " + unpopularName);
                break;
            case 2:
                // top 10 most popular male names
                babyNames.filter(e -> e.getGender().toLowerCase().equals("m"))
                        .mapToPair(n -> new Tuple2<>(n.getName(), n.getCount()))
                        .reduceByKey((x, y) -> x + y)
                        .top(10, COMPARATOR)
                        .forEach(System.out::println);
                break;
            case 3:
                // state with most children born in 1946
                String state = babyNames.filter(f -> f.getYear()==1946)
                        .mapToPair(f -> new Tuple2<>(f.getState(), f.getCount()))
                        .reduceByKey((x, y) -> x + y)
                        .max(COMPARATOR)._1;
                System.out.println("State with most children born in 1946: "+state);
                break;
            case 4:
                // list of female children count per year
                JavaPairRDD<Integer, Integer> stats = totalCounts(babyNames);
                
                stats.collect().forEach(k -> System.out.println(k._1+","+k._2));
                break;
            case 5:
                // list of percentage of children named Mary through years
                babyNames.filter(e -> e.getName().equals("Mary"))
                        .mapToPair(f -> new Tuple2<>(f.getYear(),f.getCount()))
                        .reduceByKey((x, y) -> x + y)
                        .join(totalCounts(babyNames))
                        .mapToPair(t -> new Tuple2<>(t._1, 1. * t._2._1 / t._2._2))
                        .sortByKey()
                        .collect()
                        .forEach(k -> System.out.println(k._1+","+k._2));
                break;
            case 6:
                // overall sum of children that were born
                long children = babyNames.map(USBabyNameRecord::getCount)
                        .reduce((x, y) -> x + y);
                System.out.println(children);
                break;
            case 7:
                // number of different
                long distinct = babyNames.map(USBabyNameRecord::getName)
                        .distinct().count();
                System.out.println(distinct);
                break;
            default:
                System.out.println("Specify option");
        }
    }
    
    public static JavaPairRDD<Integer, Integer> totalCounts(JavaRDD<USBabyNameRecord> babyNames){
                        JavaPairRDD<Integer, Integer> stats = babyNames
                        .filter(f -> f.getGender().toLowerCase().equals("f"))
                        .mapToPair(f -> new Tuple2<>(f.getYear(), f.getCount()))
                        .reduceByKey((x, y) -> x + y)
                        .sortByKey();
                        
                       return stats;
    }

    private static final Comparator<Tuple2<String, Integer>> COMPARATOR
            = new RecordComparator<>();

    private static final class RecordComparator<T> implements Comparator<Tuple2<T, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<T, Integer> o1, Tuple2<T, Integer> o2) {
            return Integer.compare(o1._2, o2._2);
        }
    }
}
