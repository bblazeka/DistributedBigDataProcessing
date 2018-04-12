/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mrpartitioning;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author bruno
 */
public class RideReducer extends Reducer<LongWritable, RideRecord, LongWritable, RideRecord> {
    @Override
    protected void reduce(LongWritable key, Iterable<RideRecord> values, Context context) throws IOException, InterruptedException {
        for (RideRecord value : values) {
            context.write(key, value);
        }
    }    
}
