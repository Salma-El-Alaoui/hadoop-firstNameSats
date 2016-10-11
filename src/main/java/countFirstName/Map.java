package countFirstName;

/**
 * Created by salma on 09/10/2016.
 */


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text origin_key = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String origins = value.toString().split(";")[2];
        String[] origins_array = origins.split(",");
        for (String origin : origins_array) {
            origin_key.set(origin.trim());
            context.write(origin_key, one);
        }
    }
}

