/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab4.collectionstreamshandler;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author bruno
 */
public class PollutionReading implements Serializable {

    private int ozone;
    private int particullate_matter;
    private int carbon_monoxide;
    private int sulfure_dioxide;
    private int nitrogen_dioxide;
    private double longitude;
    private double latitude;
    private Date timeStamp;

    public PollutionReading() {

    }

    static PollutionReading create(String line) {
        PollutionReading record = new PollutionReading();
        String[] entry = line.split(",");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            record.ozone = Integer.parseInt(entry[0]);
            record.particullate_matter = Integer.parseInt(entry[1]);
            record.carbon_monoxide = Integer.parseInt(entry[2]);
            record.sulfure_dioxide = Integer.parseInt(entry[3]);
            record.nitrogen_dioxide = Integer.parseInt(entry[4]);
            record.longitude = Double.parseDouble(entry[5]);
            record.latitude = Double.parseDouble(entry[6]);
            record.timeStamp = formatter.parse(entry[7]);
        } catch (NumberFormatException | ParseException e) {
            return null;
        }
        return record;
    }
    
    public long getTimestamp(){
        return timeStamp.getTime();
    }
    
    @Override
    public String toString(){
        return ozone + "," + particullate_matter + "," + carbon_monoxide + ","
                + sulfure_dioxide + "," + nitrogen_dioxide + "," + longitude
                + "," + latitude + "," + timeStamp.toString();
    }
}
