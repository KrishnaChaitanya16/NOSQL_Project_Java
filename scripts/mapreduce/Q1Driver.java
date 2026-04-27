import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1Driver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: Q1Driver <input path> <output path>");
            System.exit(-1);
        }

        // -------- Batch Info --------
        int totalRecords = 3461612;
        int batchSize = 100000;

        int numBatches = (int) Math.ceil((double) totalRecords / batchSize);

        System.out.println("Batch Size: " + batchSize);
        System.out.println("Total Records: " + totalRecords);
        System.out.println("Total Batches: " + numBatches);
        System.out.println("Average Batch Size: " + (totalRecords / numBatches));

        // -------- Hadoop Job --------
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query1");

        job.setJarByClass(Q1Driver.class);

        job.setMapperClass(Q1Mapper.class);
        job.setReducerClass(Q1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // -------- Runtime Tracking --------
        long startTime = System.currentTimeMillis();

        boolean success = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();

        long runtime = endTime - startTime;

        System.out.println("Runtime (ms): " + runtime);

        System.exit(success ? 0 : 1);
    }
}