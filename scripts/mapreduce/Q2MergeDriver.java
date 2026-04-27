import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Q2 Stage-2 Driver.
 * Reads ALL Stage-1 batch outputs (passed as comma-separated paths or
 * a glob), merges host sets globally, and emits the true Top-20.
 *
 * Usage:
 *   hadoop jar q2_merge.jar Q2MergeDriver \
 *       /etl/output/mapreduce/q2/stage1  \   ← parent dir holding batch_1..batch_N
 *       /etl/output/mapreduce/q2/final
 */
public class Q2MergeDriver {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: Q2MergeDriver <stage1-parent-dir> <output-dir>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q2 Stage-2 Global Merge");

        job.setJarByClass(Q2MergeDriver.class);

        job.setMapperClass(Q2MergeMapper.class);
        job.setReducerClass(Q2MergeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setNumReduceTasks(1);

        // Accept a glob so we read all batch_* sub-dirs in one job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
