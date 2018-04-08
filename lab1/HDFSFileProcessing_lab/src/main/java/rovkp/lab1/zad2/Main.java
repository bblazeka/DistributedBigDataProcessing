/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab1.zad2;

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
        LocalFileSystem lfs = LocalFileSystem.getLocal(conf);
        FileSystem hdfs = FileSystem.get(new URI("hdfs://cloudera2:8020"), conf);
        Path distFolder = new Path("/home/rovkp/bblazeka/gutenberg");
        Path distFile = new Path("/user/rovkp/bblazeka/gutenberg_books.txt");
        if (!lfs.isDirectory(distFolder)) {
            System.out.println("Distributed folder doesn't exist.");
        } else {
            DirectoryHandler dirHandler = new DirectoryHandler(lfs, distFolder);
            dirHandler.copyAllText(hdfs,distFile);
        }
    }
}
