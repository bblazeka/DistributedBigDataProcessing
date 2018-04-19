/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab2.mrdataseq;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author bruno
 */
public class ProcessingMapper extends Mapper<LongWritable, Text, IntWritable, DriveLog>{

    @Override
    public void setup(Context context) {

    }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        
            try {
                DriveLog log = new DriveLog(value.toString());
                context.write(new IntWritable(log.getHour()),log);
            } catch (Exception ex) {
                System.out.println("Err in processing "+ex);
            }
    }
}
