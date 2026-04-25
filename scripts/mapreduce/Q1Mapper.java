import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern pattern = Pattern.compile(
        "^(\\S+) \\S+ \\S+ \\[(.*?)\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\S+)"
    );

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        Matcher matcher = pattern.matcher(line);

        if (matcher.find()) {
            try {
                String timestamp = matcher.group(2);
                String status = matcher.group(6);
                String bytesStr = matcher.group(7);

                // Extract date (01/Jul/1995)
                String date = timestamp.split(":")[0];

                int bytes = bytesStr.equals("-") ? 0 : Integer.parseInt(bytesStr);

                // Key: date_status
                // Value: count_bytes
                context.write(
                    new Text(date + "_" + status),
                    new Text("1_" + bytes)
                );

            } catch (Exception e) {
                // skip malformed records but don't crash
            }
        }
    }
}