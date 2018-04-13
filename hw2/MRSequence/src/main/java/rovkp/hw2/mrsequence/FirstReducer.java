/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mrsequence;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author bruno
 */
public class FirstReducer extends Reducer<LongWritable, TripRecord, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<TripRecord> values, Context context) throws IOException, InterruptedException {
        for (TripRecord value : values) {
            context.write(key, new Text(value.toString()));
        }
    }    
}
