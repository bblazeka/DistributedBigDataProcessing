/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab1.hadoopserialization;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 *
 * @author bruno
 */
public class HadoopSensorAnalyzer {

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    FloatWritable value = new FloatWritable();

    public HadoopSensorAnalyzer(Configuration conf, Path file) throws IOException {
        reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
    }

    public float[] Analyze(int count) {
        int[] quantites = new int[100];
        float[] measures = new float[100];
        try {
            for (int i = 0; i < count; i++) {
                reader.next(key, value);
                quantites[key.get()-1] += 1;
                measures[key.get()-1] += value.get();
            }
            for(int i = 0; i < measures.length; i++){
                measures[i] /= quantites[i];
            }
            return measures;
        } catch (IOException ex) {
            System.err.println("Can't read lines");
        }
        return measures;
    }
}
