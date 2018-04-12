/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mrpartitioning;

/**
 *
 * @author bruno
 */
public class DEBSRecordParser {

    private String medallion;
    private Integer passengers; 
    private double pickupLong;
    private double pickupLat;
    private double dropoffLong;
    private double dropoffLat;

    public void parse(String record) {
        String[] splitted = record.split(",");
        medallion = splitted[0];
        passengers = Integer.parseInt(splitted[7]);
        pickupLong=Double.parseDouble(splitted[10]);
        pickupLat=Double.parseDouble(splitted[11]);
        dropoffLong=Double.parseDouble(splitted[12]);
        dropoffLat=Double.parseDouble(splitted[13]);
    }

    public String getMedallion() {
        return medallion;
    }

        public int getPassengers() {
        return passengers;
    }
    

    public double getPickupLong() {
        return pickupLong;
    }

    public double getPickupLat() {
        return pickupLat;
    }

    public double getDropoffLong() {
        return dropoffLong;
    }

    public double getDropoffLat() {
        return dropoffLat;
    }
    
    public RideRecord getEntry(){
        return new RideRecord(passengers,pickupLat,pickupLong,dropoffLat,dropoffLong);
    }
}
