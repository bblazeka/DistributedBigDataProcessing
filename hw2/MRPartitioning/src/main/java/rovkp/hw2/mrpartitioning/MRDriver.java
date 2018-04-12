/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mrpartitioning;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author bruno
 */
public class MRDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(MRDriver.class);
        job.setJobName("MRPartitioner Example");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(RideMapper.class);
        job.setPartitionerClass(RidePartitioner.class);
        job.setReducerClass(RideReducer.class);
        job.setNumReduceTasks(6);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(RideRecord.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);        
    }
}
