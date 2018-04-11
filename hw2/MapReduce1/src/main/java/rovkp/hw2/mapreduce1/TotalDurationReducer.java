/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mapreduce1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author bruno
 */
public class TotalDurationReducer extends Reducer<Text, TripDuration, Text, TripDuration>{
    
    @Override
    public void reduce(Text key, Iterable<TripDuration> values, Context context) throws IOException, InterruptedException{
        int max = 0, min = 0, count = 0, time = 0;
        boolean empty = true;
        for (TripDuration value : values){
            if(empty){
                max = value.getMax();
                min = value.getMin();
                empty = false;
            }
            else{
                if(value.getMax() > max){
                    max = value.getMax();
                }
                if(value.getMin() < min){
                    min = value.getMin();
                }
            }
            count += 1;
            time += value.getTime();
        }
        context.write(key, new TripDuration(min, max, count, time));
    }
}
