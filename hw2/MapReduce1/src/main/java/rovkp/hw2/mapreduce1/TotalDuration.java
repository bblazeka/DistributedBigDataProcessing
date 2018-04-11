/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mapreduce1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author bruno
 */
public class TotalDuration {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TotalDistance <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(TotalDuration.class);
        job.setJobName("Total duration");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TotalDurationMapper.class);
        job.setCombinerClass(TotalDurationReducer.class);
        job.setReducerClass(TotalDurationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TripDuration.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
