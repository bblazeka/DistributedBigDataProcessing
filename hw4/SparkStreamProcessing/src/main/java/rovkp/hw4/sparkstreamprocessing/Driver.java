/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw4.sparkstreamprocessing;

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

    public static void main(String[] args) throws InterruptedException, Exception {
        String input = "sensorscope-monitor-all.csv";
        String output = "output";
        if (args.length == 2) {
            input = args[0];
            output = args[1];
        }

        SparkConf conf = new SparkConf().setAppName("SparkStreamProcessing");

        //set the master if not already set through the command line
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            //spark streaming application requires at least 2 threads
            conf.setMaster("local[2]");
        }

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 10002);

        SensorStreamGenerator.main(new String[] {input});
        lines.map(SensorscopeReading::create)
                .filter(Objects::nonNull)
                .mapToPair(r -> new Tuple2<>(r.getStationID(), r.getSolarPanelCurrent()))
                .reduceByKeyAndWindow(Math::max, Durations.seconds(60), Durations.seconds(10))
                .dstream()
                .saveAsTextFiles(output, "");

        jssc.start();
        jssc.awaitTermination();
    }
}
