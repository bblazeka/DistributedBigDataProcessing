/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw4.collectionstreams;

import java.io.IOException;

/**
 *
 * @author bruno
 */
public class Main {
    private static final String FOLDER_PATH = "/home/bruno/rovkp/dz4/sensorscope-monitor";
    private static final String DEFINITION_FILE = "sensorscope-monitor-def.txt";
    private static final String OUTPUT = "sensorscope-monitor-all.csv";
    public static void main(String[] args) {
        SensorscopeHandler handler = new SensorscopeHandler(FOLDER_PATH,DEFINITION_FILE);
        try{
            handler.loadAndSortData(OUTPUT);
        } catch (IOException ioe){
            System.err.println("Error while reading files or writing to the file");
        }
    }
}
