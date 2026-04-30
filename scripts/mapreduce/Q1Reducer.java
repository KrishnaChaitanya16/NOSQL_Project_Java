import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q1Reducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long requestCount = 0;
        long totalBytes = 0;

        for (Text val : values) {
            String[] parts = val.toString().split("_");

            requestCount += Long.parseLong(parts[0]);
            totalBytes += Long.parseLong(parts[1]);
        }

        // Output: date_status    requestCount_totalBytes
        context.write(key, new Text(requestCount + "_" + totalBytes));
    }
}