/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab1.hadoopserialization;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author bruno
 */
public class Main {
    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI("hdfs://cloudera2:8020"), conf);
        Path file = new Path("/user/rovkp/bblazeka/ocitanja.bin");
        MeasuresGenerator mg = new MeasuresGenerator(conf,file);
        if(mg.generate(100000)){
            // file creation finished
            HadoopSensorAnalyzer analyzer = new HadoopSensorAnalyzer(conf, file);
            float[] values = analyzer.Analyze(100000);
            for(int i = 0; i < 100; i++){
                System.out.println(i+1+" "+values[i]);
            }
        }
    }
}
