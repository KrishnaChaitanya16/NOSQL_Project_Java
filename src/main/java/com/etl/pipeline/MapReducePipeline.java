package com.etl.pipeline;

import com.etl.db.ResultLoader;
import com.etl.util.BatchSplitter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

public class MapReducePipeline implements Pipeline {

    // ----------------------------------------------------------------
    // Q1 – Daily request count + total bytes per status code
    // ----------------------------------------------------------------
    @Override
    public void runQ1() throws Exception {

        System.out.println("Running MapReduce Q1...");

        List<String> batches = BatchSplitter.split();
        long totalRuntime = 0;
        long totalMalformed = 0;

        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String inputPath  = batches.get(batchId - 1);
            String outputPath = "/etl/output/mapreduce/q1/batch_" + batchId;

            System.out.println("\n--- MapReduce Q1 Batch " + batchId + "/" + batches.size() + " ---");
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

            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                if (line.contains("MALFORMED_RECORDS=")) {
                    try {
                        String val = line.substring(line.indexOf("MALFORMED_RECORDS=") + 18).trim();
                        totalMalformed += Long.parseLong(val);
                    } catch (Exception e) {}
                }
            }

            int exitCode = p.waitFor();
            System.out.println("MapReduce Q1 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Q1 MapReduce Batch " + batchId + " failed!");

            long batchRuntime = System.currentTimeMillis() - start;
            totalRuntime += batchRuntime;
            System.out.println("Batch " + batchId + " runtime: " + batchRuntime + " ms");

            ResultLoader.loadQ1(batchId, batches.size(), totalRuntime);
        }

        System.out.println("\nMapReduce Q1 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q1: " + totalMalformed);
        System.out.printf("Total batches: %d  |  Avg batch size: %.0f lines%n",
            batches.size(), (double) BatchSplitter.BATCH_SIZE);
    }

    // ----------------------------------------------------------------
    // Q2 – Top 20 most-requested resources  (TWO-STAGE)
    // Stage 1: per-batch aggregation (all resources, with host list)
    // Stage 2: global merge → true Top-20, exact distinct hosts
    // ----------------------------------------------------------------
    @Override
    public void runQ2() throws Exception {

        System.out.println("Running MapReduce Q2 (Two-Stage)...");

        List<String> batches = BatchSplitter.split();
        long totalRuntime = 0;
        long totalMalformed = 0;

        // ---- Stage 1: one job per batch ----
        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String inputPath  = batches.get(batchId - 1);
            String outputPath = "/etl/output/mapreduce/q2/stage1/batch_" + batchId;

            System.out.println("\n--- MapReduce Q2 Stage-1 Batch " + batchId + "/" + batches.size() + " ---");

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
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                if (line.contains("MALFORMED_RECORDS=")) {
                    try {
                        String val = line.substring(line.indexOf("MALFORMED_RECORDS=") + 18).trim();
                        totalMalformed += Long.parseLong(val);
                    } catch (Exception e) {}
                }
            }

            int exitCode = p.waitFor();
            System.out.println("MapReduce Q2 Stage-1 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Q2 Stage-1 Batch " + batchId + " failed!");

            totalRuntime += System.currentTimeMillis() - start;
        }

        // ---- Stage 2: global merge across all batch outputs ----
        String stage1Dir  = "/etl/output/mapreduce/q2/stage1";
        String finalOutput = "/etl/output/mapreduce/q2/final";

        System.out.println("\n--- MapReduce Q2 Stage-2 Global Merge ---");
        runCommand("hdfs", "dfs", "-rm", "-r", finalOutput);

        long mergeStart = System.currentTimeMillis();
        ProcessBuilder pb2 = new ProcessBuilder(
            "hadoop", "jar",
            "scripts/mapreduce/q2_merge.jar",
            "Q2MergeDriver",
            stage1Dir + "/*",      // glob: reads all batch_* sub-dirs
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

        // Load final merged result into DB with batch_id = total batches processed
        ResultLoader.loadQ2(batches.size(), batches.size(), totalRuntime);

        System.out.println("\nMapReduce Q2 completed (Two-Stage). Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q2: " + totalMalformed);
    }

    // ----------------------------------------------------------------
    // Q3 – Error rate per hour
    // ----------------------------------------------------------------
    @Override
    public void runQ3() throws Exception {

        System.out.println("Running MapReduce Q3...");

        List<String> batches = BatchSplitter.split();
        long totalRuntime = 0;
        long totalMalformed = 0;

        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String inputPath  = batches.get(batchId - 1);
            String outputPath = "/etl/output/mapreduce/q3/batch_" + batchId;

            System.out.println("\n--- MapReduce Q3 Batch " + batchId + "/" + batches.size() + " ---");

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

            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                if (line.contains("MALFORMED_RECORDS=")) {
                    try {
                        String val = line.substring(line.indexOf("MALFORMED_RECORDS=") + 18).trim();
                        totalMalformed += Long.parseLong(val);
                    } catch (Exception e) {}
                }
            }

            int exitCode = p.waitFor();
            System.out.println("MapReduce Q3 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Q3 MapReduce Batch " + batchId + " failed!");

            long batchRuntime = System.currentTimeMillis() - start;
            totalRuntime += batchRuntime;

            ResultLoader.loadQ3(batchId, batches.size(), totalRuntime);
        }

        System.out.println("\nMapReduce Q3 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q3: " + totalMalformed);
    }

    // -------- Helper: fire-and-forget HDFS command --------
    private void runCommand(String... cmd) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        new BufferedReader(new InputStreamReader(p.getInputStream()))
            .lines().forEach(System.out::println);
        p.waitFor();
    }
}