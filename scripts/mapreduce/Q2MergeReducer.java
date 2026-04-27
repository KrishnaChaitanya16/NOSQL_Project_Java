import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.*;

/**
 * Q2 Stage-2 Reducer.
 * Merges all per-batch aggregates for a resource path, takes the global
 * union of host sets (eliminating cross-batch duplicates), then keeps
 * the global Top-20 by request count using a min-heap.
 *
 * Input per key (resource_path):
 *   values: requestCount_totalBytes_host1,host2,...   (one per batch)
 *
 * Output (tab-separated):
 *   resource_path \t globalCount_globalBytes_globalDistinctHosts
 */
public class Q2MergeReducer extends Reducer<Text, Text, Text, Text> {

    static class Record {
        String path;
        long   requestCount;
        long   totalBytes;
        int    distinctHosts;

        Record(String path, long requestCount, long totalBytes, int distinctHosts) {
            this.path         = path;
            this.requestCount = requestCount;
            this.totalBytes   = totalBytes;
            this.distinctHosts = distinctHosts;
        }
    }

    private PriorityQueue<Record> pq;

    @Override
    protected void setup(Context context) {
        // min-heap on requestCount so we can evict the smallest
        pq = new PriorityQueue<>(Comparator.comparingLong(r -> r.requestCount));
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long globalCount = 0;
        long globalBytes = 0;
        HashSet<String> globalHosts = new HashSet<>();

        for (Text val : values) {
            try {
                // Format: count_bytes_host1,host2,...
                String s = val.toString();
                int first  = s.indexOf('_');
                int second = s.indexOf('_', first + 1);

                if (first == -1 || second == -1) continue;

                long count = Long.parseLong(s.substring(0, first));
                long bytes = Long.parseLong(s.substring(first + 1, second));
                String hostsCsv = s.substring(second + 1);

                globalCount += count;
                globalBytes += bytes;

                // Union of hosts across batches — eliminates duplicates
                if (!hostsCsv.isEmpty()) {
                    for (String h : hostsCsv.split(",")) {
                        if (!h.isEmpty()) globalHosts.add(h);
                    }
                }
            } catch (Exception e) {
                // skip bad records
            }
        }

        int distinctHosts = globalHosts.size();

        pq.add(new Record(key.toString(), globalCount, globalBytes, distinctHosts));
        if (pq.size() > 20) {
            pq.poll(); // evict resource with smallest global request count
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        List<Record> list = new ArrayList<>(pq);
        list.sort((a, b) -> Long.compare(b.requestCount, a.requestCount)); // descending

        for (Record r : list) {
            context.write(
                new Text(r.path),
                new Text(r.requestCount + "_" + r.totalBytes + "_" + r.distinctHosts)
            );
        }
    }
}
