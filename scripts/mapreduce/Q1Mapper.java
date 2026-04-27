import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    enum LogCounter {
        MALFORMED_RECORDS
    }

    // Outer CLF pattern — accepts any request string inside quotes (protocol is optional)
    private static final Pattern LOG_PATTERN = Pattern.compile(
        "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+\"(.*)\"\\s+(\\d{3})\\s+(\\S+)$"
    );

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        Matcher m = LOG_PATTERN.matcher(line);
        if (!m.find()) {
            // Cannot parse the basic CLF structure — truly malformed
            context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
            return;
        }

        // Group 2: full timestamp e.g. "01/Jul/1995:00:00:01 -0400"
        String timestamp = m.group(2);
        String status    = m.group(4);
        String bytesStr  = m.group(5);

        // Extract date (e.g. "01/Jul/1995")
        String date = timestamp.split(":")[0];

        // Treat "-" or non-numeric as 0 per spec
        long bytes = 0;
        if (!bytesStr.equals("-")) {
            try { bytes = Long.parseLong(bytesStr); } catch (NumberFormatException e) { bytes = 0; }
        }

        context.write(
            new Text(date + "_" + status),
            new Text("1_" + bytes)
        );
    }
}