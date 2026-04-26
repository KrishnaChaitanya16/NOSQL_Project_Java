import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q3Driver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: Q3Driver <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q3 Error Rate Per Hour");

        job.setJarByClass(Q3Driver.class);

        // Mapper + Reducer
        job.setMapperClass(Q3Mapper.class);
        job.setReducerClass(Q3Reducer.class);

        // Output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input / Output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Optional: single reducer for ordered output
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}