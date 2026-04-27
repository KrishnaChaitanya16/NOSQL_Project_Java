import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.HashSet;

/**
 * Q2 Stage-1 Reducer.
 * Emits ALL resource paths (no Top-20 limit) with their per-batch
 * aggregates so that the Stage-2 merge job can produce a globally
 * correct Top-20 with exact distinct-host counts.
 *
 * Output format (tab-separated):
 *   resource_path \t requestCount_totalBytes_host1,host2,...
 */
public class Q2Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long requestCount = 0;
        long totalBytes   = 0;
        HashSet<String> uniqueHosts = new HashSet<>();

        for (Text val : values) {
            try {
                String[] parts = val.toString().split("\t");
                if (parts.length < 2) continue;

                String host  = parts[0].trim();
                long   bytes = Long.parseLong(parts[1].trim());

                requestCount++;
                totalBytes += bytes;
                uniqueHosts.add(host);

            } catch (Exception e) {
                // skip bad records
            }
        }

        // Emit: count _ bytes _ comma-separated-hosts
        String hostList = String.join(",", uniqueHosts);
        context.write(
            key,
            new Text(requestCount + "_" + totalBytes + "_" + hostList)
        );
    }
}