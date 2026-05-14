package com.etl.pipeline;

import com.etl.db.ResultLoader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HivePipeline implements Pipeline {

    // ----------------------------------------------------------------
    // Q1 – Daily request count + total bytes per status code
    // ----------------------------------------------------------------
    @Override
    public void runQ1(int runId, List<String[]> batches) throws Exception {

        System.out.println("Running Hive Q1...");

        long totalRuntime  = 0;
        long totalMalformed = 0;

        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String[] batchInfo  = batches.get(batchId - 1);
            String inputPath    = batchInfo[0];
            String batchLabel   = batchInfo[1];
            String outputPath   = "/etl/output/hive/q1/batch_" + batchId;

            System.out.println("\n--- Hive Q1 Batch " + batchId + "/" + batches.size() + " [" + batchLabel + "] ---");
            System.out.println("  Input : " + inputPath);
            System.out.println("  Output: " + outputPath);

            long start = System.currentTimeMillis();

            runCommand("hdfs", "dfs", "-rm", "-r", outputPath);

            // stageInput returns the staging dir if it created one, or inputPath if the
            // input is already an HDFS directory.  We must NEVER delete inputPath.
            String stageDir  = stageInput(inputPath, "q1", batchId);
            boolean didStage = !stageDir.equals(inputPath);

            String tmpHql = renderHql("scripts/hive/q1.hql", Map.of(
                "${INPUT}",       stageDir,
                "${OUTPUT}",      outputPath,
                "${BATCH_LABEL}", batchLabel
            ));

            ProcessBuilder pb = new ProcessBuilder(
                "beeline",
                "-u", "jdbc:hive2://",
                "-f", tmpHql
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();

            long lastMalformed   = 0;
            long recordsProcessed = 0;
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                
                long mCount = parseMalformedCount(line);
                if (mCount >= 0) lastMalformed = mCount;

                long tCount = parseTotalCount(line);
                if (tCount >= 0) recordsProcessed = tCount;

                if (line.contains("Map input records=")) {
                    try {
                        String val = line.substring(line.indexOf("Map input records=") + 18).trim();
                        recordsProcessed = Long.parseLong(val);
                    } catch (Exception ignored) {}
                }
            }
            totalMalformed += lastMalformed;

            int exitCode = p.waitFor();
            Files.deleteIfExists(Paths.get(tmpHql));

            // FIX: Only remove the staging dir if WE created it – never touch inputPath
            if (didStage) {
                runCommand("hdfs", "dfs", "-rm", "-r", stageDir);
            }

            System.out.println("Hive Q1 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Q1 Hive Batch " + batchId + " failed!");

            long batchRuntime = System.currentTimeMillis() - start;
            totalRuntime += batchRuntime;
            System.out.println("Batch " + batchId + " runtime: " + batchRuntime + " ms");

            long batchSizeBytes = getHdfsFileSize(inputPath);
            int rowsInserted = ResultLoader.loadHiveQ1(runId, batchId, batches.size(), totalRuntime);
            if (recordsProcessed == 0) recordsProcessed = rowsInserted + lastMalformed;

            ResultLoader.saveBatchMeta(runId, batchId, batchLabel, batchSizeBytes, batchSizeBytes, recordsProcessed, batchRuntime);
            ResultLoader.saveMalformed(runId, batchId, "Q1", lastMalformed);
        }

        System.out.println("\nHive Q1 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q1: " + totalMalformed);
        ResultLoader.printBatchSummary("hive", runId);
    }

    // ----------------------------------------------------------------
    // Q2 – Top 20 most-requested resources  (TWO-STAGE)
    // ----------------------------------------------------------------
    @Override
    public void runQ2(int runId, List<String[]> batches) throws Exception {

        System.out.println("Running Hive Q2 (Two-Stage)...");

        long totalRuntime   = 0;
        long totalMalformed = 0;

        String stage1Dir = "/etl/output/hive/q2/stage1";
        runCommand("hdfs", "dfs", "-rm", "-r", stage1Dir);

        // ---- Stage 1: one job per batch ----
        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String[] batchInfo  = batches.get(batchId - 1);
            String inputPath    = batchInfo[0];
            String batchLabel   = batchInfo[1];
            String outputPath   = "/etl/output/hive/q2/stage1/batch_" + batchId;

            System.out.println("\n--- Hive Q2 Stage-1 Batch " + batchId + "/" + batches.size() + " [" + batchLabel + "] ---");

            long start = System.currentTimeMillis();
            runCommand("hdfs", "dfs", "-rm", "-r", outputPath);

            String stageDir  = stageInput(inputPath, "q2", batchId);
            boolean didStage = !stageDir.equals(inputPath);

            String tmpHql = renderHql("scripts/hive/q2.hql", Map.of(
                "${INPUT}",       stageDir,
                "${OUTPUT}",      outputPath,
                "${BATCH_LABEL}", batchLabel
            ));

            ProcessBuilder pb = new ProcessBuilder(
                "beeline",
                "-u", "jdbc:hive2://",
                "-f", tmpHql
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();

            long lastMalformed    = 0;
            long recordsProcessed = 0;
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                long mCount = parseMalformedCount(line);
                if (mCount >= 0) lastMalformed = mCount;

                long tCount = parseTotalCount(line);
                if (tCount >= 0) recordsProcessed = tCount;

                if (line.contains("Map input records=")) {
                    try {
                        String val = line.substring(line.indexOf("Map input records=") + 18).trim();
                        recordsProcessed = Long.parseLong(val);
                    } catch (Exception ignored) {}
                }
            }
            totalMalformed += lastMalformed;

            int exitCode = p.waitFor();
            Files.deleteIfExists(Paths.get(tmpHql));

            // FIX: Only remove the staging dir if WE created it
            if (didStage) {
                runCommand("hdfs", "dfs", "-rm", "-r", stageDir);
            }

            System.out.println("Hive Q2 Stage-1 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Q2 Stage-1 Batch " + batchId + " failed!");

            long batchRuntime = System.currentTimeMillis() - start;
            totalRuntime += batchRuntime;

            long batchSizeBytes = getHdfsFileSize(inputPath);
            ResultLoader.saveBatchMeta(runId, batchId, batchLabel, batchSizeBytes, batchSizeBytes, recordsProcessed, batchRuntime);
            ResultLoader.saveMalformed(runId, batchId, "Q2", lastMalformed);
        }

        // ---- Stage 2: global merge across all batch outputs ----
        // stage1Dir is already an HDFS directory of batch subdirs — no staging needed
        String finalOutput = "/etl/output/hive/q2/final";

        System.out.println("\n--- Hive Q2 Stage-2 Global Merge ---");
        runCommand("hdfs", "dfs", "-rm", "-r", finalOutput);

        long mergeStart = System.currentTimeMillis();

        String tmpMergeHql = renderHql("scripts/hive/q2_merge.hql", Map.of(
            "${INPUT}",  stage1Dir,
            "${OUTPUT}", finalOutput
        ));

        ProcessBuilder pb2 = new ProcessBuilder(
            "beeline",
            "-u", "jdbc:hive2://",
            "-f", tmpMergeHql
        );
        pb2.redirectErrorStream(true);
        Process p2 = pb2.start();
        new java.io.BufferedReader(new java.io.InputStreamReader(p2.getInputStream()))
            .lines().forEach(System.out::println);

        int mergeExit = p2.waitFor();
        Files.deleteIfExists(Paths.get(tmpMergeHql));
        System.out.println("Hive Q2 Stage-2 Exit Code: " + mergeExit);
        if (mergeExit != 0) throw new RuntimeException("Q2 Stage-2 global merge failed!");

        totalRuntime += System.currentTimeMillis() - mergeStart;

        ResultLoader.loadHiveQ2(runId, batches.size(), batches.size(), totalRuntime);

        System.out.println("\nHive Q2 completed (Two-Stage). Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q2: " + totalMalformed);
        ResultLoader.printBatchSummary("hive", runId);
    }

    // ----------------------------------------------------------------
    // Q3 – Error rate per hour
    // ----------------------------------------------------------------
    @Override
    public void runQ3(int runId, List<String[]> batches) throws Exception {

        System.out.println("Running Hive Q3...");

        long totalRuntime   = 0;
        long totalMalformed = 0;

        for (int batchId = 1; batchId <= batches.size(); batchId++) {
            String[] batchInfo  = batches.get(batchId - 1);
            String inputPath    = batchInfo[0];
            String batchLabel   = batchInfo[1];
            String outputPath   = "/etl/output/hive/q3/batch_" + batchId;

            System.out.println("\n--- Hive Q3 Batch " + batchId + "/" + batches.size() + " [" + batchLabel + "] ---");

            long start = System.currentTimeMillis();

            runCommand("hdfs", "dfs", "-rm", "-r", outputPath);

            String stageDir  = stageInput(inputPath, "q3", batchId);
            boolean didStage = !stageDir.equals(inputPath);

            String tmpHql = renderHql("scripts/hive/q3.hql", Map.of(
                "${INPUT}",       stageDir,
                "${OUTPUT}",      outputPath,
                "${BATCH_LABEL}", batchLabel
            ));

            ProcessBuilder pb = new ProcessBuilder(
                "beeline",
                "-u", "jdbc:hive2://",
                "-f", tmpHql
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();

            long lastMalformed    = 0;
            long recordsProcessed = 0;
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                long mCount = parseMalformedCount(line);
                if (mCount >= 0) lastMalformed = mCount;

                long tCount = parseTotalCount(line);
                if (tCount >= 0) recordsProcessed = tCount;

                if (line.contains("Map input records=")) {
                    try {
                        String val = line.substring(line.indexOf("Map input records=") + 18).trim();
                        recordsProcessed = Long.parseLong(val);
                    } catch (Exception ignored) {}
                }
            }
            totalMalformed += lastMalformed;

            int exitCode = p.waitFor();
            Files.deleteIfExists(Paths.get(tmpHql));

            // FIX: Only remove the staging dir if WE created it
            if (didStage) {
                runCommand("hdfs", "dfs", "-rm", "-r", stageDir);
            }

            System.out.println("Hive Q3 Batch " + batchId + " Exit Code: " + exitCode);
            if (exitCode != 0) throw new RuntimeException("Q3 Hive Batch " + batchId + " failed!");

            long batchRuntime = System.currentTimeMillis() - start;
            totalRuntime += batchRuntime;

            long batchSizeBytes = getHdfsFileSize(inputPath);
            int rowsInserted = ResultLoader.loadHiveQ3(runId, batchId, batches.size(), totalRuntime);
            if (recordsProcessed == 0) recordsProcessed = rowsInserted + lastMalformed;

            ResultLoader.saveBatchMeta(runId, batchId, batchLabel, batchSizeBytes, batchSizeBytes, recordsProcessed, batchRuntime);
            ResultLoader.saveMalformed(runId, batchId, "Q3", lastMalformed);
        }

        System.out.println("\nHive Q3 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q3: " + totalMalformed);
        ResultLoader.printBatchSummary("hive", runId);
    }

    // ----------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------

    /**
     * Hive LOCATION requires a directory, not a bare file path.
     * Uses "hdfs dfs -test -d" (shell CLI) so we always check the real HDFS,
     * regardless of how the JVM's Hadoop Configuration was initialised.
     *
     * Returns:
     *   • inputPath itself  – if it is already an HDFS directory (no cleanup needed)
     *   • a fresh staging dir – if it is a file (caller must delete this dir after use)
     */
    private String stageInput(String inputPath, String query, int batchId) throws Exception {
        // FIX: use CLI instead of Java API – bare new Configuration() resolves paths
        //      against local FS (file:///), not HDFS, so isDirectory() was wrong.
        boolean isDir = isHdfsDirectory(inputPath);

        if (isDir) {
            // Already an HDFS directory – use directly.
            // IMPORTANT: the caller must NOT delete this path.
            return inputPath;
        }

        // It's a file: copy into a dedicated staging directory.
        String stageDir = "/etl/hive/stage/" + query + "/batch_" + batchId;
        System.out.println("  Staging " + inputPath + " -> " + stageDir);
        runCommand("hdfs", "dfs", "-rm",   "-r", stageDir);
        runCommand("hdfs", "dfs", "-mkdir", "-p", stageDir);
        runCommand("hdfs", "dfs", "-cp",    inputPath, stageDir + "/");
        return stageDir;
    }

    /**
     * Returns true when the HDFS path is a directory.
     * "hdfs dfs -test -d" exits 0 for directories, non-zero otherwise.
     */
    private boolean isHdfsDirectory(String path) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("hdfs", "dfs", "-test", "-d", path);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        // drain output so the process doesn't block on a full pipe buffer
        new BufferedReader(new InputStreamReader(p.getInputStream())).lines().forEach(l -> {});
        return p.waitFor() == 0;
    }

    /**
     * Reads a HQL template, substitutes all ${KEY} placeholders, writes to a
     * temp file, and returns its local path.
     */
    private String renderHql(String templatePath, Map<String, String> vars) throws Exception {
        String content = new String(Files.readAllBytes(Paths.get(templatePath)));
        for (Map.Entry<String, String> entry : vars.entrySet()) {
            content = content.replace(entry.getKey(), entry.getValue());
        }
        // Strip "SET hive.variable.substitute" – no longer needed with pre-rendered HQL
        content = content.replaceAll("(?m)^\\s*SET\\s+hive\\.variable\\.substitute\\s*=.*\\n?", "");

        java.io.File tmp = java.io.File.createTempFile("hive_rendered_", ".hql");
        try (BufferedWriter w = new BufferedWriter(new FileWriter(tmp))) {
            w.write(content);
        }
        return tmp.getAbsolutePath();
    }

    /**
     * Returns the total size (bytes) of an HDFS path via "hdfs dfs -du -s".
     * FIX: previously used Java API with bare Configuration() which targeted
     *      local FS and always returned 0.
     */
    private long getHdfsFileSize(String path) {
        try {
            ProcessBuilder pb = new ProcessBuilder("hdfs", "dfs", "-du", "-s", path);
            pb.redirectErrorStream(false);
            Process p = pb.start();
            BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = r.readLine();   // format: "<size>  <disk_size>  <path>"
            p.waitFor();
            if (line != null) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length > 0) return Long.parseLong(parts[0]);
            }
        } catch (Exception ignored) {}
        return 0;
    }

    private long parseMalformedCount(String line) {
        Pattern p = Pattern.compile("MALFORMED_RECORDS=(\\d+)");
        Matcher m = p.matcher(line);
        if (m.find()) {
            return Long.parseLong(m.group(1));
        }
        return -1;
    }

    private long parseTotalCount(String line) {
        Pattern p = Pattern.compile("TOTAL_RECORDS=(\\d+)");
        Matcher m = p.matcher(line);
        if (m.find()) {
            return Long.parseLong(m.group(1));
        }
        return -1;
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
