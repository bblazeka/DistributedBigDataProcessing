/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package filereadingexample;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 *
 * @author bruno
 */
public class DirectoryHandler {

    private final File folder;
    Charset utf8 = StandardCharsets.UTF_8;

    DirectoryHandler(String dir) {
        folder = new File(dir);
        if (folder.exists()) {
            if (folder.isFile()) {
                System.err.println("Not a file, a directory");
            }
            System.out.println("File acquired");
        } else {
            System.err.println("Folder doesn't exist");
        }
    }

    private long copyTextFromFiles(BufferedWriter writer, File folder) {
        long lines = 0;
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles) {
            String line = "";
            if (file.isDirectory()) {
                lines+=copyTextFromFiles(writer, file);
            } else {
                try {
                    BufferedReader buffReader = new BufferedReader(new FileReader(file));
                    while (line != null) {
                        line = buffReader.readLine();
                        writer.write(line+System.lineSeparator());
                        lines++;
                    }
                } catch (Exception ex) {
                    System.err.println("Error while reading a file");
                }
                System.out.println(file.getName());
            }
        }
        return lines;
    }

    public void copyAllText(String destination) {
        try {
            File outputFile = new File(destination);
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
            long lines = copyTextFromFiles(writer, folder);
            System.out.println("Overall copied lines: "+lines);
        } catch (Exception ex) {
            System.err.println("Error with r/w to a file");
        }

    }

}
