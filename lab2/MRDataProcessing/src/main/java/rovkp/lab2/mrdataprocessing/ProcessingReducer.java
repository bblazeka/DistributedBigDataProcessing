/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab2.mrdataprocessing;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author bruno
 */
public class ProcessingReducer extends Reducer<IntWritable, CellAmount, NullWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<CellAmount> values, Context context) throws IOException, InterruptedException {
        double[][] amounts = new double[151][151];
        int[][] count = new int[151][151];

        for (CellAmount value : values) {
            count[value.getLong()][value.getLat()] += 1;
            amounts[value.getLong()][value.getLat()] += value.getAmount();
        }
        
        // find max
        int iMaxAmount = 0, jMaxAmount = 0, iMaxCount = 0, jMaxCount = 0, maxCount=0;
        double maxAmount=0;
        for (int i = 1; i < 151; i++){
            for (int j = 1; j < 151; j++){
                if(amounts[i][j] > maxAmount){
                    maxAmount = amounts[i][j];
                    iMaxAmount = i;
                    jMaxAmount = j;
                }
                
                if(count[i][j] > maxCount){
                    maxCount = count[i][j];
                    iMaxCount = i;
                    jMaxCount = j;
                }
            }
        }
        
        context.write(NullWritable.get(), new Text(key.toString()));
        context.write(NullWritable.get(), new Text(iMaxAmount+" "+jMaxAmount+" "+maxAmount));
        context.write(NullWritable.get(), new Text(iMaxCount+" "+jMaxCount+" "+maxCount));
    }
}
