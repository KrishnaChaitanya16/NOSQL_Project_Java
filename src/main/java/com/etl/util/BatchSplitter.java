package com.etl.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * BatchSplitter: Reads raw input files from HDFS and partitions them into
 * equal-sized batch files (by line count) under /etl/input/batches/.
 *
 * This is pure pipeline orchestration — no data transformation, filtering,
 * or cleaning is performed. Each batch file contains the exact raw log lines.
 */
public class BatchSplitter {

    public static final int    BATCH_SIZE  = 500_000;
    public static final String BATCHES_DIR = "/etl/input/batches";
    public static final String INPUT_DIR   = "/etl/input";

    /**
     * Splits the raw HDFS input into batch files.
     * @return List of HDFS paths to each batch file, in order.
     */
    public static List<String> split() throws Exception {

        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        // -------- Clean up any previous batch run --------
        Path batchesPath = new Path(BATCHES_DIR);
        if (fs.exists(batchesPath)) {
            fs.delete(batchesPath, true);
        }
        fs.mkdirs(batchesPath);

        // -------- Collect all input files --------
        FileStatus[] statuses = fs.listStatus(new Path(INPUT_DIR));

        List<String> batchPaths = new ArrayList<>();
        int batchId      = 1;
        int lineCount    = 0;
        long totalLines  = 0;

        FSDataOutputStream currentOut = null;
        String             currentPath = null;

        for (FileStatus status : statuses) {
            // Skip the batches subdirectory itself
            if (status.getPath().toString().contains(BATCHES_DIR)) continue;
            // Skip hidden files (_SUCCESS, .crc, etc.)
            String name = status.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;

            BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(status.getPath()))
            );

            String line;
            while ((line = br.readLine()) != null) {

                // ---- Open new batch file if needed ----
                if (currentOut == null) {
                    currentPath = BATCHES_DIR + "/batch_" + batchId + ".txt";
                    currentOut  = fs.create(new Path(currentPath), true);
                    System.out.println("  Creating batch " + batchId + ": " + currentPath);
                }

                currentOut.writeBytes(line + "\n");
                lineCount++;
                totalLines++;

                // ---- Close batch file when full ----
                if (lineCount == BATCH_SIZE) {
                    currentOut.close();
                    batchPaths.add(currentPath);
                    System.out.println("  Batch " + batchId + " closed: " + lineCount + " lines.");
                    batchId++;
                    lineCount  = 0;
                    currentOut = null;
                }
            }

            br.close();
        }

        // ---- Close the final (possibly partial) batch ----
        if (currentOut != null) {
            currentOut.close();
            batchPaths.add(currentPath);
            System.out.println("  Batch " + batchId + " closed (final): " + lineCount + " lines.");
        }

        fs.close();

        System.out.println("BatchSplitter: " + totalLines + " total input lines → "
                           + batchPaths.size() + " batches (batch size = " + BATCH_SIZE + ")");
        System.out.printf("Average batch size: %.0f%n",
                           (double) totalLines / batchPaths.size());

        return batchPaths;
    }
}
