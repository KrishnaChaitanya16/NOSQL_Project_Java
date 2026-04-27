package com.etl.pipeline;

import com.etl.db.ResultLoader;
import com.etl.util.BatchSplitter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

public class PigPipeline implements Pipeline {

    // ----------------------------------------------------------------
    // Q1 – Daily request count + total bytes per status code
    // ----------------------------------------------------------------
    @Override
    public void runQ1() throws Exception {

        System.out.println("Running Pig Q1...");

        // -------- Split input into batches --------
        List<String> batches = BatchSplitter.split();
        long totalRuntime = 0;
        long totalMalformed = 0;

        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String inputPath  = batches.get(batchId - 1);
            String outputPath = "/etl/output/pig/q1/batch_" + batchId;
            String outputMalformed = outputPath + "_malformed";

            System.out.println("\n--- Pig Q1 Batch " + batchId + "/" + batches.size() + " ---");
            System.out.println("  Input : " + inputPath);
            System.out.println("  Output: " + outputPath);

            long start = System.currentTimeMillis();

            // -------- Delete old output for this batch --------
            runCommand("hdfs", "dfs", "-rm", "-r", outputPath, outputMalformed);

            // -------- Run Pig script with params --------
            ProcessBuilder pb = new ProcessBuilder(
                "pig", "-x", "mapreduce",
                "-param", "INPUT="  + inputPath,
                "-param", "OUTPUT=" + outputPath,
                "-param", "OUTPUT_MALFORMED=" + outputMalformed,
                "scripts/pig/q1.pig"
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                // Pig outputs: Successfully stored 123 records in: "/etl/output/pig/q1/batch_1_malformed"
                if (line.contains("Successfully stored") && line.contains("_malformed")) {
                    try {
                        String[] parts = line.split("\\s+");
                        for (int i = 0; i < parts.length; i++) {
                            if (parts[i].equals("stored") && i + 1 < parts.length) {
                                totalMalformed += Long.parseLong(parts[i + 1]);
                                break;
                            }
                        }
                    } catch (Exception e) {}
                }
            }

            int exitCode = p.waitFor();
            System.out.println("Pig Q1 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Pig Q1 Batch " + batchId + " failed!");

            long batchRuntime = System.currentTimeMillis() - start;
            totalRuntime += batchRuntime;
            System.out.println("Batch " + batchId + " runtime: " + batchRuntime + " ms");

            // -------- Load this batch's results into DB --------
            ResultLoader.loadPigQ1(batchId, batches.size(), totalRuntime);
        }

        System.out.println("\nPig Q1 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q1: " + totalMalformed);
        System.out.printf("Total batches: %d  |  Avg batch size: %.0f lines%n",
            batches.size(), (double) BatchSplitter.BATCH_SIZE);
    }

    // ----------------------------------------------------------------
    // Q2 – Top 20 most-requested resources (TWO-STAGE)
    // Stage 1: per-batch aggregation via q2.pig
    // Stage 2: global merge via q2_merge.pig
    // ----------------------------------------------------------------
    @Override
    public void runQ2() throws Exception {

        System.out.println("Running Pig Q2 (Two-Stage)...");

        List<String> batches = BatchSplitter.split();
        long totalRuntime = 0;
        long totalMalformed = 0;

        // ---- Stage 1: one Pig job per batch ----
        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String inputPath  = batches.get(batchId - 1);
            String outputPath = "/etl/output/pig/q2/stage1/batch_" + batchId;
            String outputMalformed = outputPath + "_malformed";

            System.out.println("\n--- Pig Q2 Stage-1 Batch " + batchId + "/" + batches.size() + " ---");

            long start = System.currentTimeMillis();
            runCommand("hdfs", "dfs", "-rm", "-r", outputPath, outputMalformed);

            ProcessBuilder pb = new ProcessBuilder(
                "pig", "-x", "mapreduce",
                "-param", "INPUT="  + inputPath,
                "-param", "OUTPUT=" + outputPath,
                "-param", "OUTPUT_MALFORMED=" + outputMalformed,
                "scripts/pig/q2.pig"
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                if (line.contains("Successfully stored") && line.contains("_malformed")) {
                    try {
                        String[] parts = line.split("\\s+");
                        for (int i = 0; i < parts.length; i++) {
                            if (parts[i].equals("stored") && i + 1 < parts.length) {
                                totalMalformed += Long.parseLong(parts[i + 1]);
                                break;
                            }
                        }
                    } catch (Exception e) {}
                }
            }

            int exitCode = p.waitFor();
            System.out.println("Pig Q2 Stage-1 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Pig Q2 Stage-1 Batch " + batchId + " failed!");

            totalRuntime += System.currentTimeMillis() - start;
        }

        // ---- Stage 2: global merge ----
        String stage1Dir   = "/etl/output/pig/q2/stage1";
        String finalOutput = "/etl/output/pig/q2/final";

        System.out.println("\n--- Pig Q2 Stage-2 Global Merge ---");
        runCommand("hdfs", "dfs", "-rm", "-r", finalOutput);

        long mergeStart = System.currentTimeMillis();
        ProcessBuilder pb2 = new ProcessBuilder(
            "pig", "-x", "mapreduce",
            "-param", "INPUT="  + stage1Dir,    // Pig reads all files recursively
            "-param", "OUTPUT=" + finalOutput,
            "scripts/pig/q2_merge.pig"
        );
        pb2.redirectErrorStream(true);
        Process p2 = pb2.start();
        new java.io.BufferedReader(new java.io.InputStreamReader(p2.getInputStream()))
            .lines().forEach(System.out::println);

        int mergeExit = p2.waitFor();
        System.out.println("Pig Q2 Stage-2 Exit Code: " + mergeExit);
        if (mergeExit != 0) throw new RuntimeException("Pig Q2 Stage-2 global merge failed!");

        totalRuntime += System.currentTimeMillis() - mergeStart;

        ResultLoader.loadPigQ2(batches.size(), batches.size(), totalRuntime);

        System.out.println("\nPig Q2 completed (Two-Stage). Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q2: " + totalMalformed);
    }

    // ----------------------------------------------------------------
    // Q3 – Error rate per hour
    // ----------------------------------------------------------------
    @Override
    public void runQ3() throws Exception {

        System.out.println("Running Pig Q3...");

        List<String> batches = BatchSplitter.split();
        long totalRuntime = 0;
        long totalMalformed = 0;

        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String inputPath  = batches.get(batchId - 1);
            String outputPath = "/etl/output/pig/q3/batch_" + batchId;
            String outputMalformed = outputPath + "_malformed";

            System.out.println("\n--- Pig Q3 Batch " + batchId + "/" + batches.size() + " ---");

            long start = System.currentTimeMillis();

            runCommand("hdfs", "dfs", "-rm", "-r", outputPath, outputMalformed);

            ProcessBuilder pb = new ProcessBuilder(
                "pig", "-x", "mapreduce",
                "-param", "INPUT="  + inputPath,
                "-param", "OUTPUT=" + outputPath,
                "-param", "OUTPUT_MALFORMED=" + outputMalformed,
                "scripts/pig/q3.pig"
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                if (line.contains("Successfully stored") && line.contains("_malformed")) {
                    try {
                        String[] parts = line.split("\\s+");
                        for (int i = 0; i < parts.length; i++) {
                            if (parts[i].equals("stored") && i + 1 < parts.length) {
                                totalMalformed += Long.parseLong(parts[i + 1]);
                                break;
                            }
                        }
                    } catch (Exception e) {}
                }
            }

            int exitCode = p.waitFor();
            System.out.println("Pig Q3 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Pig Q3 Batch " + batchId + " failed!");

            long batchRuntime = System.currentTimeMillis() - start;
            totalRuntime += batchRuntime;

            ResultLoader.loadPigQ3(batchId, batches.size(), totalRuntime);
        }

        System.out.println("\nPig Q3 completed. Total runtime: " + totalRuntime + " ms");
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
