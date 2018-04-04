/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab1.hdfsfileprocessing;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author bruno
 */
public class Main {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        LocalFileSystem fs = LocalFileSystem.getLocal(conf);
        FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        
        Path localFolder = new Path("/home/bruno/ROVKP_DZ1/");
        Path localFile = new Path("/home/bruno/ROVKP_DZ1/gutenberg.zip");
        
        if(fs.isDirectory(localFolder) && fs.isFile(localFile)){
            System.out.println("Local folder and file exist.");
        } else {
            System.out.println("Local file not found");
        }
        
        Path distFolder = new Path("/user/rovkp");
        Path distFile = new Path("/user/rovkp/gutenberg.zip");
        
        if(hdfs.isDirectory(distFolder) && hdfs.isFile(distFile)){
            System.out.println("Distributed folder and file exist.");
        }
        else {
            System.out.println("Distributed folder not found");
        }
        
    }
}
