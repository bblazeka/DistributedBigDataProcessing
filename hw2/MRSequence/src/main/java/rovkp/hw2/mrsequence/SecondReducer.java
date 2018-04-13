/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mrsequence;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author bruno
 */
public class SecondReducer extends Reducer<Text, TripDuration, Text, TripDuration>{
    
    @Override
    public void reduce(Text key, Iterable<TripDuration> values, Context context) throws IOException, InterruptedException{
        int max = 0, min = 0, count = 0, time = 0;
        String[] id = key.toString().split(" ");
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
        mos.write("bins", id[0], new TripDuration(min, max, count, time), "part"+id[1]);
        context.write(key, new TripDuration(min, max, count, time));
    }
    
    
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        mos.close();
    }
    
    
    private MultipleOutputs<Text, TripDuration> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }
}
