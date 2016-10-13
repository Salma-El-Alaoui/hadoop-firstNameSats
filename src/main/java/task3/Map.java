package task3;

/**
 * Created by salma on 12/10/2016.
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<Object, Text, Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);
    private final static LongWritable zero = new LongWritable(0);
    private final static String MALE = "m";
    private final static String FEMALE= "f";
    private Text genderKey = new Text();


    /**
     * @param value: line from the file which is structured as such: name; [genders]; [origins] ; version
     * writes: list(key = gender (f or m) , value = 1 for the encountered gender and 0 for the opposite gender) for genders encountered in the line
     *         if a names has two genders, we only count it once (which means the 0 for the opposite gender only appears once in the list).
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String genders = value.toString().split(";")[1];
        String[] gendersArray = genders.split(",");
        int counter = 0;
        for (String gender : gendersArray) {
            if(gender.equals(FEMALE)){
                genderKey.set(gender.trim());
                context.write(genderKey, one);
                //we only increment the total number of names once (in case the name has both genders)
                if(counter == 0)
                    context.write(new Text(MALE), zero);
            }
            else if(gender.equals(MALE)) {
                genderKey.set(gender.trim());
                context.write(genderKey, one);
                if(counter == 0)
                    context.write(new Text(FEMALE), zero);
            }
            counter++;
        }
    }
}

