/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab2.mrdataprocessing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author bruno
 */
public class CellAmount implements WritableComparable<CellAmount>{
    
    private IntWritable longCell;
    private IntWritable latCell;
    private DoubleWritable amount;
    
    public CellAmount(){
        longCell = new IntWritable();
        latCell = new IntWritable();
        amount = new DoubleWritable();        
    }
    
    public CellAmount(int lng, int lat, double amnt){
        longCell = new IntWritable(lng);
        latCell = new IntWritable(lat);
        amount = new DoubleWritable(amnt);
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

    @Override
    public void write(DataOutput d) throws IOException {
        longCell.write(d);
        latCell.write(d);
        amount.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        longCell.readFields(di);
        latCell.readFields(di);
        amount.readFields(di);
    }

    @Override
    public int compareTo(CellAmount o) {
        return o.amount.compareTo(amount) + o.latCell.compareTo(latCell)
                + o.longCell.compareTo(longCell);
    }
}
