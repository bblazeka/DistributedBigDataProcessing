/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab2.mrdataseq;

import java.io.*;
import org.apache.hadoop.io.*;

/**
 *
 * @author bruno
 */
public class DriveLog implements WritableComparable<DriveLog>{
    
    private IntWritable longCell;
    private IntWritable latCell;
    private DoubleWritable amount;
    private Text pickup;
    
    public DriveLog(){
        pickup = new Text();
        longCell = new IntWritable();
        latCell = new IntWritable();
        amount = new DoubleWritable();        
    }
    
    public DriveLog(String text, int lng, int lat, double amnt){
        pickup = new Text(text);
        longCell = new IntWritable(lng);
        latCell = new IntWritable(lat);
        amount = new DoubleWritable(amnt);
    }
    
    public DriveLog(String log){
        String[] logArray = log.split(",");
        pickup = new Text(logArray[0]);
        longCell = new IntWritable(Integer.parseInt(logArray[1]));
        latCell = new IntWritable(Integer.parseInt(logArray[2]));
        amount = new DoubleWritable(Double.parseDouble(logArray[3]));
    }
    
    public int getLat(){
        return latCell.get();
    }
    
    public int getLong(){
        return longCell.get();
    }
    
    public double getAmount(){
        return amount.get();
    }
    
    public int getHour(){
        String[] time = pickup.toString().split(" ")[1].split(":");
        return Integer.parseInt(time[0]);
    }

    @Override
    public void write(DataOutput d) throws IOException {
        longCell.write(d);
        latCell.write(d);
        amount.write(d);
        pickup.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        longCell.readFields(di);
        latCell.readFields(di);
        amount.readFields(di);
        pickup.readFields(di);
    }

    @Override
    public int compareTo(DriveLog o) {
        return o.amount.compareTo(amount) + o.latCell.compareTo(latCell)
                + o.longCell.compareTo(longCell);
    }
    
    @Override
    public String toString(){
        return pickup.toString() + "," + longCell.get() + "," + latCell.get() + "," + amount.get();
    }
}
