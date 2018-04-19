/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab2.mrdataprocessing;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author bruno
 */
public class ProcessingMapper extends Mapper<LongWritable, Text, IntWritable, CellAmount>{

    @Override
    public void setup(Context context) {

    }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        DEBSRecordParser parser = new DEBSRecordParser();
        
        if (key.get() > 0) {
            String record = value.toString();
            try {
                parser.parse(record);
                context.write(new IntWritable(parser.getHour()),parser.cellAmount());
            } catch (Exception ex) {
                System.out.println("Cannot parse: " + record + " due to the " + ex);
            }
        }
    }    
}
