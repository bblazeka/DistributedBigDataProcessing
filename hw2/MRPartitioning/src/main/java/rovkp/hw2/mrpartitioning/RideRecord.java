/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mrpartitioning;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

/**
 *
 * @author bruno
 */
public class RideRecord implements WritableComparable<RideRecord>{

    private static final double MIN_LONG=-74;
    private static final double MAX_LAT=40.75;
    private static final double MIN_LAT=-73.95;
    private static final double MAX_LONG=40.8;
    private IntWritable passengers;
    private DoubleWritable pickupLat;
    private DoubleWritable pickupLong;
    private DoubleWritable dropoffLat;
    private DoubleWritable dropoffLong;
    
    RideRecord(){
        this.pickupLat = new DoubleWritable();
        this.pickupLong = new DoubleWritable();
        this.dropoffLat = new DoubleWritable();
        this.dropoffLong = new DoubleWritable();
        this.passengers = new IntWritable();
    }
    
    RideRecord(int passengers, double pickupLat, double pickupLong,
            double dropoffLat, double dropoffLong) {
        this.pickupLat = new DoubleWritable(pickupLat);
        this.pickupLong = new DoubleWritable(pickupLong);
        this.dropoffLat = new DoubleWritable(dropoffLat);
        this.dropoffLong = new DoubleWritable(dropoffLong);
        this.passengers = new IntWritable(passengers);
    }
    
    public boolean checkLatitude(){
        return pickupLat.get() > MIN_LAT && pickupLat.get() < MAX_LAT &&
                dropoffLat.get() > MIN_LAT && dropoffLat.get() < MAX_LAT;
    }
    
    public boolean checkLongitude(){
        return pickupLong.get() > MIN_LONG && pickupLong.get() < MAX_LONG &&
                dropoffLong.get() > MIN_LONG && dropoffLong.get() < MAX_LONG;
    }
    
    public boolean centralRide(){
        return checkLatitude() && checkLongitude();
    }
    
    public int passengers(){
        return passengers.get();
    }

    @Override
    public void write(DataOutput d) throws IOException {
        pickupLat.write(d);
        pickupLong.write(d);
        dropoffLat.write(d);
        dropoffLong.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        pickupLat.readFields(di);
        pickupLong.readFields(di);
        dropoffLat.readFields(di);
        dropoffLong.readFields(di);
    }

    @Override
    public int compareTo(RideRecord o) {
        return this.pickupLat.compareTo(o.pickupLat)
                +this.dropoffLat.compareTo(o.dropoffLat)
                +this.pickupLong.compareTo(o.pickupLong)
                +this.dropoffLong.compareTo(o.dropoffLong);
    }
    
    @Override
    public String toString(){
        return pickupLat+" "+pickupLong+" | "+dropoffLat+" "+dropoffLong;
    }
    
}
