/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab2.mrdataseq;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

/**
 *
 * @author bruno
 */
public class Driver {

    private static final String INTERMEDIATE_PATH = "intermediate";

    public static void main(String[] args) throws IOException, InterruptedException,
            ClassNotFoundException {
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
        adJob.setJarByClass(Driver.class);
        adJob.setJobName("Filtering job");

        FileInputFormat.addInputPath(adJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(adJob, new Path(INTERMEDIATE_PATH));

        adJob.setMapperClass(FilteringMapper.class);
        adJob.setNumReduceTasks(0);
        adJob.setOutputKeyClass(NullWritable.class);
        adJob.setOutputValueClass(Text.class);

        int code = adJob.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            Job bJob = Job.getInstance();
            bJob.setJarByClass(Driver.class);
            bJob.setJobName("Analysis job");

            FileInputFormat.addInputPath(bJob, new Path(INTERMEDIATE_PATH));
            FileOutputFormat.setOutputPath(bJob, new Path(args[1]));

            bJob.setMapperClass(ProcessingMapper.class);
            bJob.setPartitionerClass(ProcessingPartitioner.class);
            bJob.setReducerClass(ProcessingReducer.class);
            bJob.setNumReduceTasks(24);

            bJob.setMapOutputKeyClass(IntWritable.class);
            bJob.setMapOutputValueClass(DriveLog.class);
            bJob.setOutputKeyClass(NullWritable.class);
            bJob.setOutputValueClass(Text.class);

            code = bJob.waitForCompletion(true) ? 0 : 1;
        }

        System.exit(code);
    }
}
