/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab1.zad2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author bruno
 */
public class DirectoryHandler {

    private final Path folder;
    private final FileSystem fs;
    private int fileCount = 0;
    Charset utf8 = StandardCharsets.UTF_8;

    DirectoryHandler(FileSystem fs,Path dir) {
        folder = dir;
        this.fs = fs;
        try{
            if(!fs.isDirectory(dir)){
                System.err.println("Folder doesn't exist");
            }
        } catch (Exception ex){
            System.err.println("can't read folder");
        }
    }

    private long copyTextFromFiles(BufferedWriter writer, Path folder) throws IOException {
        long lines = 0;
        FileStatus[] listOfFiles = fs.listStatus(folder);
        for (FileStatus file : listOfFiles) {
            String line = "";
            Path path = file.getPath();
            if (file.isDirectory()) {
                lines+=copyTextFromFiles(writer, path);
            } else {
                try {
                    BufferedReader buffReader = new BufferedReader(
                            new InputStreamReader(fs.open(path)));
                    while (line != null) {
                        line = buffReader.readLine();
                        writer.write(line+System.lineSeparator());
                        lines++;
                    }
                } catch (Exception ex) {
                    System.err.println("Error while reading a file");
                }
                fileCount++;
                System.out.println(file.getPath());
            }
        }
        return lines;
    }

    public void copyAllText(FileSystem destFS, Path destination) {
        try {
            BufferedWriter writer = 
                    new BufferedWriter(new OutputStreamWriter(destFS.create(destination)));
            fileCount = 0;
            long lines = copyTextFromFiles(writer, folder);
            System.out.println("Overall copied lines: "+lines);
            System.out.println("Overall copied files: "+fileCount);
        } catch (Exception ex) {
            System.err.println("Error with r/w to a file");
        }

    }

}
