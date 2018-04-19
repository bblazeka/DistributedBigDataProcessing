/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab2.mrdataprocessing;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author bruno
 */
public class ProcessingPartitioner extends Partitioner<IntWritable, CellAmount> {

    @Override
    public int getPartition(IntWritable key, CellAmount value, int i) {
        return key.get();
    }
    
}
