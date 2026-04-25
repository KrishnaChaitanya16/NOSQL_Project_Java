import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.net.URLDecoder;

public class Q2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    enum LogCounter {
        MALFORMED_RECORDS
    }

    private Text resource = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        try {
            // ---------- BASIC SPLIT ----------
            String[] parts = line.split("\\s+");
            if (parts.length < 10){
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
    return;
            }

            String host = parts[0];

            // ---------- EXTRACT REQUEST ----------
            int firstQuote  = line.indexOf('"');
            int secondQuote = line.indexOf('"', firstQuote + 1);
            if (firstQuote == -1 || secondQuote == -1){
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }

            String request = line.substring(firstQuote + 1, secondQuote);
            // Expected format: GET /path HTTP/1.0

            String[] reqParts = request.split(" ");
            if (reqParts.length < 3){
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }            // must have METHOD + PATH + PROTOCOL

            String method = reqParts[0];
            String path   = reqParts[1];

            // Only handle standard HTTP methods
            if (!method.equals("GET") && !method.equals("POST") && !method.equals("HEAD")) {
              
                return;
            }

            // ---------- URL DECODE ----------
            try {
                path = URLDecoder.decode(path, "UTF-8");
            } catch (Exception e) {
                // ignore decoding issues
            }

            // ---------- CLEAN ----------
            path = path.replaceAll("[^\\x20-\\x7E]", ""); // strip non-printable
            path = path.trim();

            // ---------- FILTER ----------
            if (!path.startsWith("/"))  {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }
            if (path.startsWith("//")) {
    context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
    return;
}
            if (path.length() < 2)     {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }
            if (path.equals("/"))       {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }
            if (path.contains(".."))    {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }
            if (path.contains("*"))     {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }   // wildcard paths like /*, /*.*
            if (path.contains(" "))     {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }   // malformed paths with spaces
            if (path.matches("^/\\..*")) {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                return;
            }  // hidden paths like /., /.html, /.ksc.html

            // ---------- BYTES (fixed column index = 9) ----------
            // NASA log format: host ident authuser date request status bytes
            // parts[9] is bytes (0-indexed), parts[8] is status code
            long bytes = 0;
            String byteStr = parts[9];

            if (!byteStr.equals("-")) {
                try {
                    bytes = Long.parseLong(byteStr);
                    if (bytes < 0) bytes = 0;   // guard against malformed negatives
                } catch (NumberFormatException e) {
                    bytes = 0;
                }
            }

            // ---------- EMIT ----------
            resource.set(path);
            outValue.set(host + "\t" + bytes);
            context.write(resource, outValue);

        } catch (Exception e) {
                context.getCounter(LogCounter.MALFORMED_RECORDS).increment(1);
                //
            // skip bad lines safely
        }
    }
}