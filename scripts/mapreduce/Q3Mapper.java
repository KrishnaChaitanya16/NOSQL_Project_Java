import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Q3Mapper extends Mapper<LongWritable, Text, Text, Text> {

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

        String host      = m.group(1);
        String timestamp = m.group(2); // e.g. "01/Aug/1995:00:00:01 -0400"
        String statusStr = m.group(4);

        // Extract date and hour from timestamp
        String[] dtParts = timestamp.split(":");
        if (dtParts.length < 2) return; // edge case, skip silently
        String date = dtParts[0];
        String hour = dtParts[1];

        int status;
        try {
            status = Integer.parseInt(statusStr);
        } catch (NumberFormatException e) {
            return; // regex already guarantees \d{3}, guard just in case
        }

        String outKey = date + "_" + hour;

        if (status >= 400 && status <= 599) {
            context.write(new Text(outKey), new Text("1_1_" + host));
        } else {
            context.write(new Text(outKey), new Text("1_0_"));
        }
    }
}