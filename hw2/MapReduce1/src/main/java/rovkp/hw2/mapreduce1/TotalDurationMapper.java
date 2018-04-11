/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mapreduce1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author bruno
 */
public class TotalDurationMapper extends Mapper<LongWritable, Text, Text, TripDuration> {
    @Override
    public void map(LongWritable key, Text value, Context context){
        DEBSRecordParser parser = new DEBSRecordParser();
        
        if (key.get() > 0) {
            String record = value.toString();
            try {
                parser.parse(record);
                context.write(new Text(parser.getMedallion()), new TripDuration(1,parser.getTime()));
            } catch (Exception ex) {
                //System.out.println("Cannot parse: " + record + "due to the " + ex);
            }
        }
    }
}
