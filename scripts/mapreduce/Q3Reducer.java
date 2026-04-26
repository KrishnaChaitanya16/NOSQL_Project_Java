import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class Q3Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long totalRequests = 0;
        long errorRequests = 0;

        for (Text val : values) {
            try {
                String[] parts = val.toString().split("_");

                totalRequests += Long.parseLong(parts[0]);
                errorRequests += Long.parseLong(parts[1]);

            } catch (Exception e) {
                // skip bad values
            }
        }

        if (totalRequests == 0) return;

        double errorRate = (double) errorRequests / totalRequests;

        // limit decimal precision (clean output)
        String formattedRate = String.format("%.5f", errorRate);

        context.write(
                key,
                new Text(totalRequests + "_" + formattedRate)
        );
    }
}