/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab4.collectionstreamshandler;

import java.io.IOException;

/**
 *
 * @author bruno
 */
public class Main {

    private static final String FOLDER_PATH = "/home/bruno/rovkp/lab4/pollutionData";
    private static final String OUTPUT = "pollutionData-all.csv";

    public static void main(String[] args) {
        DataHandler handler = new DataHandler(FOLDER_PATH);
        try{
            handler.loadAndSortData(OUTPUT);
        } catch (IOException ioe){
            System.err.println("Error while reading files or writing to the file");
        }
    }
}
