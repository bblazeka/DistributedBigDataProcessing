/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mapreduce1;

/**
 *
 * @author bruno
 */
public class DEBSRecordParser {

    private String medallion;
    private double distance;
    private Integer time;

    public void parse(String record) throws Exception {
        String[] splitted = record.split(",");
        medallion = splitted[0];
        distance = Double.parseDouble(splitted[9]);
        time = Integer.parseInt(splitted[8]);
    }

    public String getMedallion() {
        return medallion;
    }

    public double getDistance() {
        return distance;
    }
    
    public Integer getTime(){
        return time;
    }

}
