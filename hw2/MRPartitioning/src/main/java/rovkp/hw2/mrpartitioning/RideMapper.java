/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mrpartitioning;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author bruno
 */
public class RideMapper extends Mapper<LongWritable, Text, LongWritable, RideRecord> {

    @Override
    public void map(LongWritable key, Text value, Context context) {
        DEBSRecordParser parser = new DEBSRecordParser();

        //skip the first line
        if (key.get() > 0) {
            String record = value.toString();
            try {
                parser.parse(record);
                context.write(key, parser.getEntry());
            } catch (IOException | InterruptedException ex) {
                System.out.println("Parse exception: " + ex 
                        + " while parsing: " + record);
            }
        }
    }
}
