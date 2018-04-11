/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mapreduce1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author bruno
 */
public class TripDuration implements WritableComparable<TripDuration> {

    private IntWritable min;
    private IntWritable max;
    private IntWritable overall;
    private IntWritable count;
    
    public TripDuration(){
        min = new IntWritable();
        max = new IntWritable();
        overall = new IntWritable();
        count = new IntWritable();
    }

    public TripDuration(int min, int max, int count, int time){
        this.min = new IntWritable(min);
        this.max = new IntWritable(max);
        this.overall = new IntWritable(time);
        this.count = new IntWritable(count);
    }
    
    public TripDuration(int count, Integer time) {
        this.overall = new IntWritable(time);
        this.count = new IntWritable(count);
        this.max = new IntWritable(time);
        this.min = new IntWritable(time);
    }
    
    public Integer getMin(){
        return min.get();
    }
    
    public Integer getMax(){
        return max.get();
    }
    
    public Integer getTime(){
        return overall.get();
    }
    
    public Integer getCount(){
        return count.get();
    }

    @Override
    public void write(DataOutput d) throws IOException {
        min.write(d);
        max.write(d);
        overall.write(d);
        count.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        min.readFields(di);
        max.readFields(di);
        overall.readFields(di);
        count.readFields(di);
    }

    @Override
    public int compareTo(TripDuration o) {
        return overall.compareTo(o.overall);
    }
    
    @Override
    public String toString(){
        return overall+" "+min+" "+max;
    }

}
