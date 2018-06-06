/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw4.collectionstreams;

import java.io.Serializable;
import java.util.Date;

/**
 *
 * @author bruno
 */
    public class SensorscopeReading implements Serializable {

        private int stationID;
        private int year;
        private int month;
        private int day;
        private int hour;
        private int minute;
        private int second;
        private int timesincetheepoch;
        private int sequenceNumber;
        private double configSamplingTime;
        private double dataSamplingTime;
        private double radioDutyCycle;
        private double radioTransmissionPower;
        private double radioTransmissionFrequency;
        private double primaryBufferVoltage;
        private double secondaryBufferVoltage;
        private double solarPanelCurrent;
        private double globalCurrent;
        private double energySource;
        
        public SensorscopeReading(){
            
        }

        static SensorscopeReading create(String line) {
            String[] entry = line.split(" ");
            SensorscopeReading record = new SensorscopeReading();
            try {
                record.stationID = Integer.parseInt(entry[0]);
                record.year = Integer.parseInt(entry[1]);
                record.month = Integer.parseInt(entry[2]);
                record.day = Integer.parseInt(entry[3]);
                record.hour = Integer.parseInt(entry[4]);
                record.minute = Integer.parseInt(entry[5]);
                record.second = Integer.parseInt(entry[6]);
                record.timesincetheepoch = Integer.parseInt(entry[7]);
                record.sequenceNumber = Integer.parseInt(entry[8]);
                record.configSamplingTime = Double.parseDouble(entry[9]);
                record.dataSamplingTime = Double.parseDouble(entry[10]);
                record.radioDutyCycle = Double.parseDouble(entry[11]);
                record.radioTransmissionPower = Double.parseDouble(entry[12]);
                record.radioTransmissionFrequency = Double.parseDouble(entry[13]);
                record.primaryBufferVoltage = Double.parseDouble(entry[14]);
                record.secondaryBufferVoltage = Double.parseDouble(entry[15]);
                record.solarPanelCurrent = Double.parseDouble(entry[16]);
                record.globalCurrent = Double.parseDouble(entry[17]);
                record.energySource = Double.parseDouble(entry[18]);
            } catch (NumberFormatException e) {
                return null;
            }
            return record;
        }

        public long getTime(){
            Date date = new Date(year, month, day, hour, minute, second);
            return date.getTime();
        }

        @Override
        public String toString() {
            return stationID + "," + year + "," + month + "," + day + "," + hour + "," + minute + ","
                    + second + "," + timesincetheepoch + "," + sequenceNumber + ","
                    + configSamplingTime + "," + dataSamplingTime + "," + radioDutyCycle + ","
                    + radioTransmissionPower + "," + radioTransmissionFrequency + ","
                    + primaryBufferVoltage + "," + secondaryBufferVoltage + ","
                    + solarPanelCurrent + "," + globalCurrent + "," + energySource;
        }
    }