import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.*;

public class Q2Reducer extends Reducer<Text, Text, Text, Text> {

    // ---------- Record class ----------
    static class Record {
        String path;
        long requestCount;
        long totalBytes;
        int distinctHosts;

        Record(String path, long requestCount, long totalBytes, int distinctHosts) {
            this.path = path;
            this.requestCount = requestCount;
            this.totalBytes = totalBytes;
            this.distinctHosts = distinctHosts;
        }
    }

    // ---------- Min Heap (Top 20) ----------
    private PriorityQueue<Record> pq;

    @Override
    protected void setup(Context context) {
        pq = new PriorityQueue<>(Comparator.comparingLong(r -> r.requestCount));
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long requestCount = 0;
        long totalBytes = 0;
        HashSet<String> uniqueHosts = new HashSet<>();

        for (Text val : values) {
            try {
                String[] parts = val.toString().split("\\t");
                if (parts.length < 2) continue;

                String host = parts[0].trim();
                long bytes = Long.parseLong(parts[1].trim());

                requestCount++;
                totalBytes += bytes;
                uniqueHosts.add(host);

            } catch (Exception e) {
                // skip bad records
            }
        }

        int distinctHosts = uniqueHosts.size();

        // ---------- Add to Top-20 heap ----------
        pq.add(new Record(key.toString(), requestCount, totalBytes, distinctHosts));

        if (pq.size() > 20) {
            pq.poll(); // remove smallest
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        // Convert heap to list
        List<Record> list = new ArrayList<>(pq);

        // Sort descending
        list.sort((a, b) -> Long.compare(b.requestCount, a.requestCount));

        // Output final Top 20
        for (Record r : list) {
            context.write(
                new Text(r.path),
                new Text(r.requestCount + "_" + r.totalBytes + "_" + r.distinctHosts)
            );
        }
    }
}