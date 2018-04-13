/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rovkp.hw2.mrsequence;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author bruno
 */
public class SecondMapper extends Mapper<LongWritable, Text, Text, TripDuration> {

    @Override
    public void map(LongWritable key, Text value, Context context) {
        IntermediateParser parser = new IntermediateParser();

        if (key.get() > 0) {
            String record = value.toString();
            try {
                parser.parse(record);
                int group = group(parser.getEntry());
                context.write(new Text(parser.getMedallion()+" "+group),
                        new TripDuration(1, parser.getTime()));
            } catch (IOException | InterruptedException ex) {
                System.out.println("Cannot parse: " + record + " due to the " + ex);
            }
        }
    }

    public int group(TripRecord record) throws IOException, InterruptedException {

        int factor;
        if (record.passengers() >= 4) {
            factor = 2;
        } else if (record.passengers() > 1) {
            factor = 1;
        } else {
            factor = 0;
        }

        if (record.centralRide()) {
            return 2 * factor;
        } else {
            return 2 * factor + 1;
        }
    }
}
