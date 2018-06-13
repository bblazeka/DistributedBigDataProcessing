/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab4.sparkstreampollution;

import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 *
 * @author bruno
 */
public class Driver {

    private static final String INPUT = "pollutionData-all.csv";
    private static final String OUTPUT = "output";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("SparkStreamPollution");
        String input, output;
        if (args.length == 2) {
            input = args[0];
            output = args[1];
        } else {
            input = INPUT;
            output = OUTPUT;
        }
        
        //set the master if not already set through the command line
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            //spark streaming application requires at least 2 threads
            conf.setMaster("local[2]");
        }

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 10002);

        SensorStreamGenerator.main(new String[]{input});
        lines.map(PollutionReading::create)
                .filter(Objects::nonNull)
                .mapToPair(r -> new Tuple2<>(r.generateId(), r.getOzone()))
                .reduceByKeyAndWindow(Math::min, Durations.seconds(40), Durations.seconds(15))
                .dstream()
                .saveAsTextFiles(output, "");

        jssc.start();
        jssc.awaitTermination();
    }
}
