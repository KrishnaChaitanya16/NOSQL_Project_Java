import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.HashSet;

public class Q3Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long totalRequests = 0;
        long errorRequests = 0;
        HashSet<String> distinctErrorHosts = new HashSet<>();

        for (Text val : values) {
            try {
                // Format: "total_error_host"
                String[] parts = val.toString().split("_", 3);

                totalRequests += Long.parseLong(parts[0]);
                errorRequests += Long.parseLong(parts[1]);

                // Add host only for error records with a valid host
                if (parts[1].equals("1") && parts.length == 3 && !parts[2].isEmpty()) {
                    distinctErrorHosts.add(parts[2]);
                }

            } catch (Exception e) {
                // skip bad values
            }
        }

        if (totalRequests == 0) return;

        double errorRate = (double) errorRequests / totalRequests;
        String formattedRate = String.format("%.5f", errorRate);

        // Output columns: log_date_hour | total_requests _ error_requests _ error_rate _ distinct_error_hosts
        context.write(
                key,
                new Text(totalRequests + "_" + errorRequests + "_" + formattedRate + "_" + distinctErrorHosts.size())
        );
    }
}