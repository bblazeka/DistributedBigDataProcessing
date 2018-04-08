/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab1.hadoopserialization;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author bruno
 */
public class MeasuresGenerator {
    
    SequenceFile.Writer writer;
    Writable key = new IntWritable();
    Writable value = new FloatWritable();
    public MeasuresGenerator(Configuration conf, Path file) throws IOException{
        writer = SequenceFile.createWriter(conf, 
                SequenceFile.Writer.file(file),
                SequenceFile.Writer.keyClass(key.getClass()),
                SequenceFile.Writer.valueClass(value.getClass()));
    }
    
    public boolean generate(int measuresCount){
        try {
            Random rand = new Random();
            for(int i = 0; i < measuresCount; i++){
                key = new IntWritable(rand.nextInt(100) + 1);
                value = new FloatWritable(rand.nextFloat() * (float)99.99);
                writer.append(key, value);
            }
            writer.close();
            return true;
        } catch (IOException ex) {
            System.out.println("Can't create a file");
            return false;
        }
    }
}
