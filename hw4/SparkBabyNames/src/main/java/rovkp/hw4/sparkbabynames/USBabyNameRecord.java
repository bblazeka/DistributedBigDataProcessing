/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw4.sparkbabynames;

/**
 *
 * @author bruno
 */
public class USBabyNameRecord {
    
    private int Id;
    private String Name;
    private int Year;
    private String Gender;
    private String State;
    private int Count;
    
    public USBabyNameRecord(){
        
    }
    
    public static USBabyNameRecord create(String line){
        String[] entry = line.split(",");
        USBabyNameRecord record = new USBabyNameRecord();
        try{
            record.Id = Integer.parseInt(entry[0]);
            record.Name = entry[1];
            record.Year = Integer.parseInt(entry[2]);
            record.Gender = entry[3];
            record.State = entry[4];
            record.Count = Integer.parseInt(entry[5]);
        } catch (NumberFormatException nfe){
            return null;
        }
        return record;
    }
    
    public String getName(){
        return Name;
    }
    
    public String getGender(){
        return Gender;
    }
    
    public int getCount(){
        return Count;
    }
    
    public int getYear(){
        return Year;
    }
    
    public String getState(){
        return State;
    }
    
    @Override
    public String toString(){
        return Name+","+Year+","+Gender+","+State+","+Count;
    }
}
