/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab2.mrdataseq;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author bruno
 */
public class ProcessingPartitioner extends Partitioner<IntWritable, DriveLog> {

    @Override
    public int getPartition(IntWritable key, DriveLog value, int i) {
        return key.get();
    }
    
}
