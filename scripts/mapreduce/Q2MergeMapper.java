import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * Q2 Stage-2 Mapper.
 * Reads Stage-1 output lines and re-emits them keyed by resource path.
 *
 * Input line format (tab-separated):
 *   resource_path \t requestCount_totalBytes_host1,host2,...
 *
 * Output:
 *   key  = resource_path
 *   value = requestCount_totalBytes_host1,host2,...
 */
public class Q2MergeMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // Stage-1 output is tab-separated: path \t count_bytes_hosts
        int tab = line.indexOf('\t');
        if (tab == -1) return;

        String path    = line.substring(0, tab).trim();
        String payload = line.substring(tab + 1).trim();

        if (path.isEmpty() || payload.isEmpty()) return;

        context.write(new Text(path), new Text(payload));
    }
}
