package countNumberOrigins;

/**
 * Created by salma on 12/10/2016.
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text numberOrigins = new Text();

    /**
     * @param value: line from the file which is structured as such: name; [genders]; [origins] ; version
     * writes: list(key = number of origins, value = 1)
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String origins = value.toString().split(";")[2];
        String[] originsArray = origins.split(",");
        numberOrigins.set(Integer.toString(originsArray.length));
        context.write(numberOrigins, one);
    }
}

