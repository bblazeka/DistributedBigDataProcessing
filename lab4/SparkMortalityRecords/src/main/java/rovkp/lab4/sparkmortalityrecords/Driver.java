/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab4.sparkmortalityrecords;

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

    private static final String INPUT = "/home/bruno/rovkp/lab4/DeathRecords.csv";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MortalityRecords");

        //set the master if not already set through the command line
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local");
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<USDeathRecord> deathRecords = sc.textFile(INPUT)
                .map(USDeathRecord::create)
                .filter(Objects::nonNull);

        switch (7) {
            case 1:
                // female over 40
                long femaleOver40 = deathRecords
                        .filter(e -> e.getGender().toLowerCase().equals("f")
                        && e.getAge() > 40)
                        .count();
                System.out.println("Female over 40: " + femaleOver40);
                break;
            case 2:
                // month with most males under 50
                int month = deathRecords.filter(f -> f.getGender().toLowerCase().equals("m")
                        && f.getAge() < 50)
                        .mapToPair(r -> new Tuple2<>(r.getMonthOfDeath(), 1L))
                        .reduceByKey((x, y) -> x + y)
                        .max(COMPARATOR)._1;
                System.out.println("Month with most dead males under 50: " + month);
                break;
            case 3:
                // females that underwent autopsy
                long autopsyFemale = deathRecords
                        .filter(f -> f.getGender().toLowerCase().equals("f")
                        && f.getAutopsy())
                        .count();
                System.out.println("Females that underwent autopsy: " + autopsyFemale);
                break;
            case 4:
                // mortality of females[50,65] in weekdays
                JavaPairRDD<Integer, Long> monthMovement
                        = female5065Mortality(deathRecords);
                monthMovement.collect().forEach(k -> System.out.println(k._1+","+k._2));
                break;
            case 5:
                // married women percentage, per weekday
                JavaPairRDD<Integer, Double> married = female5065(deathRecords)
                        .filter(f -> "M".equals(f.getMaritalStatus().toUpperCase()))
                        .mapToPair(r -> new Tuple2<>(r.getDayOfDeath(), 1L))
                        .reduceByKey((x, y) -> x + y)
                        .sortByKey()
                        .join(female5065Mortality(deathRecords))
                        .mapToPair(t -> new Tuple2<>(t._1, 1. * t._2._1 / t._2._2));

                married.collect().forEach(k -> System.out.println(k._1+","+k._2));
                break;
            case 6:
                // men that died in accidents
                long accidents = deathRecords
                        .filter(f -> f.getGender().toLowerCase().equals("m")
                                && f.getCauseOfDeath() == 1)
                        .count();
                System.out.println("Men that died in accidents: "+accidents);
                break;
            case 7:
                // distinct ages
                long ages = deathRecords.map(USDeathRecord::getAge).distinct().count();
                System.out.println("Existing distinct ages: "+ages);
                break;
            default:
                System.out.println("Specify option");
        }
    }

    private static JavaRDD<USDeathRecord> female5065(JavaRDD<USDeathRecord> records) {
        return records.filter(f -> f.getGender().toLowerCase().equals("f")
                && f.getAge() > 50 && f.getAge() < 65);
    }

    private static JavaPairRDD<Integer, Long> female5065Mortality(JavaRDD<USDeathRecord> records) {
        return female5065(records)
                .mapToPair(r -> new Tuple2<>(r.getDayOfDeath(), 1L))
                .reduceByKey((x, y) -> x + y)
                .sortByKey();
    }

    private static final Comparator<Tuple2<Integer, Long>> COMPARATOR
            = new RecordComparator<>();

    private static final class RecordComparator<T> implements Comparator<Tuple2<T, Long>>, Serializable {

        @Override
        public int compare(Tuple2<T, Long> o1, Tuple2<T, Long> o2) {
            return Long.compare(o1._2, o2._2);
        }
    }
}
