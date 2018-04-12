/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mrpartitioning;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author bruno
 */
public class RidePartitioner extends Partitioner<LongWritable, RideRecord> {

    @Override
    public int getPartition(LongWritable key, RideRecord record, int i) {
        int factor;
        if(record.passengers() >= 4){
            factor = 2;
        } else if (record.passengers() > 1) {
            factor = 1;
        } else {
            factor = 0;
        }
        
        if(record.centralRide()){
            return 2*factor;
        } else {
            return 2*factor+1;
        }
    }
    
}
