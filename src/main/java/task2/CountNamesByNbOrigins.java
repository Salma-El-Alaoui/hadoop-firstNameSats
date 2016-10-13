package task2;

/**
 * Created by salma on 12/10/2016.
 * This MapReduce job counts the number of names by origin count.
 * A missing/unknown origin doesn't induce a count : names with a "?" origin or no origin
 * are considered to have 0 origin.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountNamesByNbOrigins {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "count number of first name by number of origins");
        job.setJarByClass(CountNamesByNbOrigins.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
