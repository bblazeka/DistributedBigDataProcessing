/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mrsequence;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

/**
 *
 * @author bruno
 */
public class SequentialDriver {

    private static final String INTERMEDIATE_PATH = "intermediate";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length != 2) {
            System.err.println("Usage: <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        try {
            FileSystem.get(conf).delete(new Path(INTERMEDIATE_PATH), true);
        } catch (Exception ex) {
            System.out.println("Intermediate file doesn't exist. Continue...");
        }

        Job adJob = Job.getInstance();
        adJob.setJarByClass(SequentialDriver.class);
        adJob.setJobName("Partitioning job");

        adJob.setMapperClass(FirstMapper.class);
        adJob.setPartitionerClass(FirstPartitioner.class);
        adJob.setReducerClass(FirstReducer.class);
        adJob.setNumReduceTasks(6);

        FileInputFormat.addInputPath(adJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(adJob, new Path(INTERMEDIATE_PATH));

        adJob.setMapOutputKeyClass(LongWritable.class);
        adJob.setMapOutputValueClass(TripRecord.class);
        adJob.setOutputKeyClass(Text.class);
        adJob.setOutputValueClass(Text.class);

        int code = adJob.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            Job bJob = Job.getInstance();
            bJob.setJarByClass(SequentialDriver.class);
            bJob.setJobName("Analysis job");

            FileInputFormat.addInputPath(bJob, new Path(INTERMEDIATE_PATH));
            FileOutputFormat.setOutputPath(bJob, new Path(args[1]));

            bJob.setMapperClass(SecondMapper.class);
            bJob.setReducerClass(SecondReducer.class);

            bJob.setMapOutputKeyClass(Text.class);
            bJob.setMapOutputValueClass(TripDuration.class);
            bJob.setOutputKeyClass(Text.class);
            bJob.setOutputValueClass(TripDuration.class);

            //configure the multiple outputs
            MultipleOutputs.addNamedOutput(bJob, "bins", TextOutputFormat.class, Text.class, TripDuration.class);

            code = bJob.waitForCompletion(true) ? 0 : 1;
        }

        System.exit(code);
    }
}
