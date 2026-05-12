package com.etl.pipeline;

import com.etl.db.ResultLoader;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

public class MapReducePipeline implements Pipeline {

    // ----------------------------------------------------------------
    // Q1 – Daily request count + total bytes per status code
    // ----------------------------------------------------------------
    @Override
    public void runQ1(int runId, List<String[]> batches) throws Exception {

        System.out.println("Running MapReduce Q1...");

        long totalRuntime = 0;
        long totalMalformed = 0;

        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String[] batchInfo = batches.get(batchId - 1);
            String inputPath  = batchInfo[0];
            String batchLabel = batchInfo[1];
            String outputPath = "/etl/output/mapreduce/q1/batch_" + batchId;

            System.out.println("\n--- MapReduce Q1 Batch " + batchId + "/" + batches.size() + " [" + batchLabel + "] ---");
            System.out.println("  Input : " + inputPath);
            System.out.println("  Output: " + outputPath);

            long start = System.currentTimeMillis();

            runCommand("hdfs", "dfs", "-rm", "-r", outputPath);

            ProcessBuilder pb = new ProcessBuilder(
                "hadoop", "jar",
                "scripts/mapreduce/q1.jar",
                "Q1Driver",
                inputPath,
                outputPath
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();

            long lastMalformed = 0;
            long recordsProcessed = 0;
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                if (line.contains("MALFORMED_RECORDS=")) {
                    try {
                        String val = line.substring(line.indexOf("MALFORMED_RECORDS=") + 18).trim();
                        lastMalformed = Long.parseLong(val);
                    } catch (Exception e) {}
                }
                if (line.contains("Map input records=")) {
                    try {
                        String val = line.substring(line.indexOf("Map input records=") + 18).trim();
                        recordsProcessed = Long.parseLong(val);
                    } catch (Exception e) {}
                }
            }
            totalMalformed += lastMalformed;

            int exitCode = p.waitFor();
            System.out.println("MapReduce Q1 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Q1 MapReduce Batch " + batchId + " failed!");

            long batchRuntime = System.currentTimeMillis() - start;
            totalRuntime += batchRuntime;
            System.out.println("Batch " + batchId + " runtime: " + batchRuntime + " ms");

            long batchSizeBytes = getHdfsFileSize(inputPath);
            ResultLoader.saveBatchMeta(runId, batchId, batchLabel, batchSizeBytes, batchSizeBytes, recordsProcessed, batchRuntime);
            ResultLoader.saveMalformed(runId, batchId, "Q1", lastMalformed);

            ResultLoader.loadQ1(runId, batchId, batches.size(), totalRuntime);
        }

        System.out.println("\nMapReduce Q1 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q1: " + totalMalformed);
        ResultLoader.printBatchSummary("mapreduce", runId);
    }

    // ----------------------------------------------------------------
    // Q2 – Top 20 most-requested resources  (TWO-STAGE)
    // ----------------------------------------------------------------
    @Override
    public void runQ2(int runId, List<String[]> batches) throws Exception {

        System.out.println("Running MapReduce Q2 (Two-Stage)...");

        long totalRuntime = 0;
        long totalMalformed = 0;

        String stage1Dir  = "/etl/output/mapreduce/q2/stage1";
        runCommand("hdfs", "dfs", "-rm", "-r", stage1Dir);

        // ---- Stage 1: one job per batch ----
        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String[] batchInfo = batches.get(batchId - 1);
            String inputPath  = batchInfo[0];
            String batchLabel = batchInfo[1];
            String outputPath = "/etl/output/mapreduce/q2/stage1/batch_" + batchId;

            System.out.println("\n--- MapReduce Q2 Stage-1 Batch " + batchId + "/" + batches.size() + " [" + batchLabel + "] ---");

            long start = System.currentTimeMillis();
            runCommand("hdfs", "dfs", "-rm", "-r", outputPath);

            ProcessBuilder pb = new ProcessBuilder(
                "hadoop", "jar",
                "scripts/mapreduce/q2.jar",
                "Q2Driver",
                inputPath,
                outputPath
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();

            long lastMalformed = 0;
            long recordsProcessed = 0;
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                if (line.contains("MALFORMED_RECORDS=")) {
                    try {
                        String val = line.substring(line.indexOf("MALFORMED_RECORDS=") + 18).trim();
                        lastMalformed = Long.parseLong(val);
                    } catch (Exception e) {}
                }
                if (line.contains("Map input records=")) {
                    try {
                        String val = line.substring(line.indexOf("Map input records=") + 18).trim();
                        recordsProcessed = Long.parseLong(val);
                    } catch (Exception e) {}
                }
            }
            totalMalformed += lastMalformed;

            int exitCode = p.waitFor();
            System.out.println("MapReduce Q2 Stage-1 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Q2 Stage-1 Batch " + batchId + " failed!");

            long batchRuntime = System.currentTimeMillis() - start;
            totalRuntime += batchRuntime;

            long batchSizeBytes = getHdfsFileSize(inputPath);
            ResultLoader.saveBatchMeta(runId, batchId, batchLabel, batchSizeBytes, batchSizeBytes, recordsProcessed, batchRuntime);
            ResultLoader.saveMalformed(runId, batchId, "Q2", lastMalformed);
        }

        // ---- Stage 2: global merge across all batch outputs ----
        // stage1Dir already defined above
        String finalOutput = "/etl/output/mapreduce/q2/final";

        System.out.println("\n--- MapReduce Q2 Stage-2 Global Merge ---");
        runCommand("hdfs", "dfs", "-rm", "-r", finalOutput);

        long mergeStart = System.currentTimeMillis();
        ProcessBuilder pb2 = new ProcessBuilder(
            "hadoop", "jar",
            "scripts/mapreduce/q2_merge.jar",
            "Q2MergeDriver",
            stage1Dir + "/*",
            finalOutput
        );
        pb2.redirectErrorStream(true);
        Process p2 = pb2.start();
        new java.io.BufferedReader(new java.io.InputStreamReader(p2.getInputStream()))
            .lines().forEach(System.out::println);

        int mergeExit = p2.waitFor();
        System.out.println("MapReduce Q2 Stage-2 Exit Code: " + mergeExit);
        if (mergeExit != 0) throw new RuntimeException("Q2 Stage-2 global merge failed!");

        totalRuntime += System.currentTimeMillis() - mergeStart;

        // Load final merged result into DB
        ResultLoader.loadQ2(runId, batches.size(), batches.size(), totalRuntime);

        System.out.println("\nMapReduce Q2 completed (Two-Stage). Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q2: " + totalMalformed);
        ResultLoader.printBatchSummary("mapreduce", runId);
    }

    // ----------------------------------------------------------------
    // Q3 – Error rate per hour
    // ----------------------------------------------------------------
    @Override
    public void runQ3(int runId, List<String[]> batches) throws Exception {

        System.out.println("Running MapReduce Q3...");

        long totalRuntime = 0;
        long totalMalformed = 0;

        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String[] batchInfo = batches.get(batchId - 1);
            String inputPath  = batchInfo[0];
            String batchLabel = batchInfo[1];
            String outputPath = "/etl/output/mapreduce/q3/batch_" + batchId;

            System.out.println("\n--- MapReduce Q3 Batch " + batchId + "/" + batches.size() + " [" + batchLabel + "] ---");

            long start = System.currentTimeMillis();

            runCommand("hdfs", "dfs", "-rm", "-r", outputPath);

            ProcessBuilder pb = new ProcessBuilder(
                "hadoop", "jar",
                "scripts/mapreduce/q3.jar",
                "Q3Driver",
                inputPath,
                outputPath
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();

            long lastMalformed = 0;
            long recordsProcessed = 0;
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                if (line.contains("MALFORMED_RECORDS=")) {
                    try {
                        String val = line.substring(line.indexOf("MALFORMED_RECORDS=") + 18).trim();
                        lastMalformed = Long.parseLong(val);
                    } catch (Exception e) {}
                }
                if (line.contains("Map input records=")) {
                    try {
                        String val = line.substring(line.indexOf("Map input records=") + 18).trim();
                        recordsProcessed = Long.parseLong(val);
                    } catch (Exception e) {}
                }
            }
            totalMalformed += lastMalformed;

            int exitCode = p.waitFor();
            System.out.println("MapReduce Q3 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Q3 MapReduce Batch " + batchId + " failed!");

            long batchRuntime = System.currentTimeMillis() - start;
            totalRuntime += batchRuntime;

            long batchSizeBytes = getHdfsFileSize(inputPath);
            ResultLoader.saveBatchMeta(runId, batchId, batchLabel, batchSizeBytes, batchSizeBytes, recordsProcessed, batchRuntime);
            ResultLoader.saveMalformed(runId, batchId, "Q3", lastMalformed);

            ResultLoader.loadQ3(runId, batchId, batches.size(), totalRuntime);
        }

        System.out.println("\nMapReduce Q3 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q3: " + totalMalformed);
        ResultLoader.printBatchSummary("mapreduce", runId);
    }

    // -------- Helpers --------

    private long getHdfsFileSize(String path) {
        try {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
            return fs.getFileStatus(new org.apache.hadoop.fs.Path(path)).getLen();
        } catch (Exception e) {
            return 0;
        }
    }

    private void runCommand(String... cmd) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        new BufferedReader(new InputStreamReader(p.getInputStream()))
            .lines().forEach(System.out::println);
        p.waitFor();
    }
}