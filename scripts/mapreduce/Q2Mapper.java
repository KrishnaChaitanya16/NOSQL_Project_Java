import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Q2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    enum LogCounter {
        MALFORMED_RECORDS
    }

    // Outer CLF pattern — accepts any request string inside quotes (protocol is optional)
    private static final Pattern LOG_PATTERN = Pattern.compile(
        "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+\"(.*)\"\\s+(\\d{3})\\s+(\\S+)$"
    );

    // Request field parser — protocol is optional
    private static final Pattern REQUEST_PATTERN = Pattern.compile(
        "^(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?$"
    );

    private Text resource = new Text();
    private Text outValue = new Text();

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

        String host    = m.group(1);
        String request = m.group(3);
        String byteStr = m.group(5);

        // Parse the request field (protocol is optional)
        Matcher req = REQUEST_PATTERN.matcher(request);
        if (!req.matches()) return; // garbage request string — skip silently

        String method = req.group(1);
        String path   = req.group(2);
        // group(3) is protocol — may be null if absent, that's fine

        // Business-logic: only standard HTTP methods
        if (!method.equals("GET") && !method.equals("POST") && !method.equals("HEAD")) return;

        // URL decode and clean path
        try { path = URLDecoder.decode(path, "UTF-8"); } catch (Exception ignored) {}
        path = path.replaceAll("[^\\x20-\\x7E]", "").trim();
        if (!path.startsWith("/")) return;

        // Treat "-" or non-numeric as 0 per spec
        long bytes = 0;
        if (!byteStr.equals("-")) {
            try { bytes = Long.parseLong(byteStr); if (bytes < 0) bytes = 0; }
            catch (NumberFormatException e) { bytes = 0; }
        }

        resource.set(path);
        outValue.set(host + "\t" + bytes);
        context.write(resource, outValue);
    }
}