import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class Q3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    enum LogCounter {
        MALFORMED_RECORDS
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        try {
            String[] parts = line.split("\\s+");

            if (parts.length < 9) {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }

            // -------- Extract host --------
            String host = parts[0];

            // -------- Extract datetime --------
            String datetime = parts[3];

            if (!datetime.startsWith("[")) {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }

            datetime = datetime.substring(1);

            String[] dtParts = datetime.split(":");
            if (dtParts.length < 2) {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }

            String date = dtParts[0];
            String hour = dtParts[1];

            // -------- Extract status --------
            int status;
            try {
                status = Integer.parseInt(parts[8]);
            } catch (Exception e) {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }

            // -------- Emit --------
            String outKey = date + "_" + hour;

            if (status >= 400) {
                // error record: total=1, error=1, host included
                context.write(new Text(outKey), new Text("1_1_" + host));
            } else {
                // non-error record: total=1, error=0, no host
                context.write(new Text(outKey), new Text("1_0_"));
            }

        } catch (Exception e) {
            context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
        }
    }
}