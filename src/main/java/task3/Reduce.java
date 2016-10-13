package task3;

/**
 * Created by salma on 12/10/2016.
 */

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce<Key> extends Reducer<Key,LongWritable, Key,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    /**
     * @param key: gender (f or m)
     * @param values: list(1 for the occurrences of the key gender and 0 for the occurrences of the opposite gender)
     * To get the number of occurrences the key gender, we sum the values of the list (i.e. the number of 1s
     * To get the number of names, we get the size of the list
     * writes: (gender, percentage of the gender in the data)
     */
    public void reduce(Key key, Iterable<LongWritable> values,
                       Context context) throws IOException, InterruptedException {
        long sum = 0;
        long numberNames = 0;
        for (LongWritable val : values) {
            sum += val.get();
            numberNames += 1;
        }

        result.set((double)sum * 100 / numberNames);
        context.write(key, result);

    }
}