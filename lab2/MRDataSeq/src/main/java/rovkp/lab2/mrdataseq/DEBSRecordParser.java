/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab2.mrdataseq;

/**
 *
 * @author bruno
 */
public class DEBSRecordParser {

    private String medallion;
    private String pickup;
    private Double amount;
    private double pickupLong;
    private double pickupLat;
    private double dropoffLong;
    private double dropoffLat;
    private final int priceLimit = 0;
    private final double beginLat = 41.474937;
    private final double beginLong = -74.913585;
    private final double gridLength = 0.011972;
    private final double gridWidth = 0.008983112;
    private int[] cellId;

    public void parse(String record) throws Exception {
        String[] splitted = record.split(",");
        medallion = splitted[0];
        pickup = splitted[2];
        pickupLong = Double.parseDouble(splitted[6]);
        pickupLat = Double.parseDouble(splitted[7]);
        dropoffLong = Double.parseDouble(splitted[8]);
        dropoffLat = Double.parseDouble(splitted[9]);
        amount = Double.parseDouble(splitted[16]);
        cellId = new int[4];
        cellId[0] = ((int) ((pickupLong - beginLong) / gridLength) + 1);
        cellId[1] = ((int) ((beginLat - pickupLat) / gridWidth) + 1);
        cellId[2] = ((int) ((dropoffLong - beginLong) / gridLength) + 1);
        cellId[3] = ((int) ((beginLat - dropoffLat) / gridWidth) + 1);
    }

    public int getHour() {
        String[] time = pickup.split(" ")[1].split(":");
        return Integer.parseInt(time[0]);
    }

    public String getMedallion() {
        return medallion;
    }

    public double getAmount() {
        return amount;
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

    public boolean checkParser() {
        if (amount <= priceLimit) {
            return false;
        }
        for (int val : cellId) {
            if (val > 151 || val < 1) {
                return false;
            }
        }
        return true;
    }

    public DriveLog drive() {
        return new DriveLog(pickup,cellId[0], cellId[1], amount);
    }
}
