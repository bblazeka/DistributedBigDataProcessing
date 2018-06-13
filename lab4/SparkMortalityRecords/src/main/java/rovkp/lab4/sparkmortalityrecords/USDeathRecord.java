/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.lab4.sparkmortalityrecords;

/**
 *
 * @author bruno
 */
public class USDeathRecord {

    private int Id;
    private int MonthOfDeath;
    private String Sex;
    private int Age;
    private String MaritalStatus;
    private int DayOfWeekOfDeath;
    private int MannerOfDeath;
    private boolean Autopsy;

    public USDeathRecord() {

    }

    static USDeathRecord create(String line) {
        USDeathRecord record = new USDeathRecord();
        String[] entry = line.split(",");
        try {
            record.Id = Integer.parseInt(entry[0]);
            record.MonthOfDeath = Integer.parseInt(entry[5]);
            record.Sex = entry[6];
            record.Age = Integer.parseInt(entry[8]);
            record.MaritalStatus = entry[15];
            record.DayOfWeekOfDeath = Integer.parseInt(entry[16]);
            record.MannerOfDeath = Integer.parseInt(entry[19]);
            record.Autopsy = "Y".equals(entry[21]);
        } catch (NumberFormatException nfe) {
            return null;
        }
        return record;
    }

    public String getGender() {
        return Sex;
    }
    
    public int getAge(){
        return Age;
    }
    
    public int getMonthOfDeath(){
        return MonthOfDeath;
    }
    
    public boolean getAutopsy(){
        return Autopsy;
    }
    
    public int getDayOfDeath(){
        return DayOfWeekOfDeath;
    }
    
    public String getMaritalStatus(){
        return MaritalStatus;
    }
    
    public int getCauseOfDeath(){
        return MannerOfDeath;
    }
}
