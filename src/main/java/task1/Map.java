package task1;

/**
 * Created by salma on 09/10/2016.
 */


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text originKey = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String origins = value.toString().split(";")[2];
        String[] originsArray = origins.split(",");
        for (String origin : originsArray) {
            originKey.set(origin.trim());
            context.write(originKey, one);
        }
    }
}

