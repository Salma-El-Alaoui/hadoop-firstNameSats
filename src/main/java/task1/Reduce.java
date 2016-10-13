package task1;

/**
 * Created by salma on 09/10/2016.
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce<Key> extends Reducer<Key,IntWritable, Key,IntWritable> {
    private IntWritable result = new IntWritable();

    /**
     * @param key: origin
     * @param values: number of occurrences of the key for each map node if a combiner is used,
     *                otherwise a list of ones (1 for each occurrence).
     * writes: (origin, number of names having this origin)
     */
    public void reduce(Key key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}