package com.etl.db;

import java.io.*;
import java.sql.*;
import java.util.Properties;

import com.etl.util.BatchSplitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ResultLoader {

    // ============================================================
    // MAPREDUCE LOADERS
    // ============================================================

    public static void loadQ1(int batchId, int totalBatches, long runtime) throws Exception {

        Properties props = new Properties();
        props.load(new FileInputStream("config/db.properties"));
        Connection conn = DriverManager.getConnection(
            props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));

        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        // Read all part files from this batch's output directory
        String outputDir = "/etl/output/mapreduce/q1/batch_" + batchId;
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO results (" +
            "pipeline_name, query_name, run_id, batch_id, " +
            "log_date, log_hour, status_code, resource_path, " +
            "request_count, total_requests, total_bytes, distinct_hosts, error_rate, runtime_ms" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        int totalInserted = 0;

        for (FileStatus part : parts) {
            String name = part.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(part.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    String[] parts2 = line.split("\\s+");
                    String[] key   = parts2[0].split("_");
                    String[] val   = parts2[1].split("_");

                    String date   = key[0];
                    int    status = Integer.parseInt(key[1]);
                    int    count  = Integer.parseInt(val[0]);
                    long   bytes  = Long.parseLong(val[1]);

                    ps.setString(1, "mapreduce");
                    ps.setString(2, "Q1");
                    ps.setInt(3, 1);
                    ps.setInt(4, batchId);
                    ps.setString(5, date);
                    ps.setNull(6, Types.INTEGER);
                    ps.setInt(7, status);
                    ps.setNull(8, Types.VARCHAR);
                    ps.setInt(9, count);
                    ps.setNull(10, Types.INTEGER);
                    ps.setLong(11, bytes);
                    ps.setNull(12, Types.INTEGER);
                    ps.setNull(13, Types.DOUBLE);
                    ps.setLong(14, runtime);

                    ps.addBatch();
                    totalInserted++;
                } catch (Exception e) { /* skip bad lines */ }
            }
            br.close();
        }

        ps.executeBatch();
        fs.close(); ps.close(); conn.close();

        System.out.printf("MapReduce Q1 Batch %d/%d → %d rows inserted%n",
            batchId, totalBatches, totalInserted);

        if (batchId == totalBatches) {
            printBatchSummary("mapreduce", "Q1", totalBatches, runtime);
            printQ1Results("mapreduce");
        }
    }

    public static void loadQ2(int batchId, int totalBatches, long runtime) throws Exception {

        Properties props = new Properties();
        props.load(new FileInputStream("config/db.properties"));
        Connection conn = DriverManager.getConnection(
            props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));

        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        // Stage-2 two-stage pipeline: read from the final merged output
        String outputDir = "/etl/output/mapreduce/q2/final";
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO results (" +
            "pipeline_name, query_name, run_id, batch_id, " +
            "resource_path, request_count, total_bytes, distinct_hosts, runtime_ms" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        int totalInserted = 0;

        for (FileStatus part : parts) {
            String name = part.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(part.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    String[] p2   = line.trim().split("\\s+", 2);
                    if (p2.length < 2) continue;
                    String[] vals = p2[1].split("_");
                    if (vals.length < 3) continue;

                    ps.setString(1, "mapreduce");
                    ps.setString(2, "Q2");
                    ps.setInt(3, 1);
                    ps.setInt(4, batchId);
                    ps.setString(5, p2[0]);
                    ps.setInt(6, Integer.parseInt(vals[0]));
                    ps.setLong(7, Long.parseLong(vals[1]));
                    ps.setInt(8, Integer.parseInt(vals[2]));
                    ps.setLong(9, runtime);

                    ps.addBatch();
                    totalInserted++;
                } catch (Exception e) { /* skip bad lines */ }
            }
            br.close();
        }

        ps.executeBatch();
        fs.close(); ps.close(); conn.close();

        System.out.printf("MapReduce Q2 Batch %d/%d → %d rows inserted%n",
            batchId, totalBatches, totalInserted);

        if (batchId == totalBatches) {
            printBatchSummary("mapreduce", "Q2", totalBatches, runtime);
            printQ2Results("mapreduce");
        }
    }

    public static void loadQ3(int batchId, int totalBatches, long runtime) throws Exception {

        Properties props = new Properties();
        props.load(new FileInputStream("config/db.properties"));
        Connection conn = DriverManager.getConnection(
            props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));

        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String outputDir = "/etl/output/mapreduce/q3/batch_" + batchId;
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO results (" +
            "pipeline_name, query_name, run_id, batch_id, " +
            "log_date, log_hour, total_requests, request_count, error_rate, distinct_hosts, runtime_ms" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        int totalInserted = 0;

        for (FileStatus part : parts) {
            String name = part.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(part.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    String[] p2 = line.trim().split("\\s+", 2);
                    if (p2.length < 2) continue;
                    String[] kp = p2[0].split("_");
                    if (kp.length < 2) continue;
                    String[] vp = p2[1].split("_");
                    if (vp.length < 4) continue; // expecting 4 values now

                    ps.setString(1, "mapreduce");
                    ps.setString(2, "Q3");
                    ps.setInt(3, 1);
                    ps.setInt(4, batchId);
                    ps.setString(5, kp[0]);
                    ps.setInt(6, Integer.parseInt(kp[1]));
                    ps.setInt(7, Integer.parseInt(vp[0])); // total requests
                    ps.setInt(8, Integer.parseInt(vp[1])); // error requests (stored in request_count)
                    ps.setDouble(9, Double.parseDouble(vp[2])); // error rate
                    ps.setInt(10, Integer.parseInt(vp[3])); // distinct error hosts
                    ps.setLong(11, runtime);

                    ps.addBatch();
                    totalInserted++;
                } catch (Exception e) { /* skip bad lines */ }
            }
            br.close();
        }

        ps.executeBatch();
        fs.close(); ps.close(); conn.close();

        System.out.printf("MapReduce Q3 Batch %d/%d → %d rows inserted%n",
            batchId, totalBatches, totalInserted);

        if (batchId == totalBatches) {
            printBatchSummary("mapreduce", "Q3", totalBatches, runtime);
            printQ3Results("mapreduce");
        }
    }

    // ============================================================
    // PIG LOADERS
    // ============================================================

    public static void loadPigQ1(int batchId, int totalBatches, long runtime) throws Exception {

        Properties props = new Properties();
        props.load(new FileInputStream("config/db.properties"));
        Connection conn = DriverManager.getConnection(
            props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));

        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String outputDir = "/etl/output/pig/q1/batch_" + batchId;
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO results (" +
            "pipeline_name, query_name, run_id, batch_id, " +
            "log_date, log_hour, status_code, resource_path, " +
            "request_count, total_requests, total_bytes, distinct_hosts, error_rate, runtime_ms" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        int totalInserted = 0;

        for (FileStatus part : parts) {
            String name = part.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(part.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    String[] parts2 = line.split("\\t");
                    if (parts2.length < 2) continue;
                    String[] key = parts2[0].split("_");
                    String[] val = parts2[1].split("_");

                    ps.setString(1, "pig");
                    ps.setString(2, "Q1");
                    ps.setInt(3, 1);
                    ps.setInt(4, batchId);
                    ps.setString(5, key[0]);
                    ps.setNull(6, Types.INTEGER);
                    ps.setInt(7, Integer.parseInt(key[1]));
                    ps.setNull(8, Types.VARCHAR);
                    ps.setInt(9, Integer.parseInt(val[0]));
                    ps.setNull(10, Types.INTEGER);
                    ps.setLong(11, Long.parseLong(val[1]));
                    ps.setNull(12, Types.INTEGER);
                    ps.setNull(13, Types.DOUBLE);
                    ps.setLong(14, runtime);

                    ps.addBatch();
                    totalInserted++;
                } catch (Exception e) { /* skip bad lines */ }
            }
            br.close();
        }

        ps.executeBatch();
        fs.close(); ps.close(); conn.close();

        System.out.printf("Pig Q1 Batch %d/%d → %d rows inserted%n",
            batchId, totalBatches, totalInserted);

        if (batchId == totalBatches) {
            printBatchSummary("pig", "Q1", totalBatches, runtime);
            printQ1Results("pig");
        }
    }

    public static void loadPigQ2(int batchId, int totalBatches, long runtime) throws Exception {

        Properties props = new Properties();
        props.load(new FileInputStream("config/db.properties"));
        Connection conn = DriverManager.getConnection(
            props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));

        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        // Stage-2 two-stage pipeline: read from the final merged output
        String outputDir = "/etl/output/pig/q2/final";
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO results (" +
            "pipeline_name, query_name, run_id, batch_id, " +
            "resource_path, request_count, total_bytes, distinct_hosts, runtime_ms" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        int totalInserted = 0;

        for (FileStatus part : parts) {
            String name = part.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(part.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    String[] p2   = line.trim().split("\\t", 2);
                    if (p2.length < 2) continue;
                    String[] vals = p2[1].split("_");
                    if (vals.length < 3) continue;

                    ps.setString(1, "pig");
                    ps.setString(2, "Q2");
                    ps.setInt(3, 1);
                    ps.setInt(4, batchId);
                    ps.setString(5, p2[0]);
                    ps.setInt(6, Integer.parseInt(vals[0]));
                    ps.setLong(7, Long.parseLong(vals[1]));
                    ps.setInt(8, Integer.parseInt(vals[2]));
                    ps.setLong(9, runtime);

                    ps.addBatch();
                    totalInserted++;
                } catch (Exception e) { /* skip bad lines */ }
            }
            br.close();
        }

        ps.executeBatch();
        fs.close(); ps.close(); conn.close();

        System.out.printf("Pig Q2 Batch %d/%d → %d rows inserted%n",
            batchId, totalBatches, totalInserted);

        if (batchId == totalBatches) {
            printBatchSummary("pig", "Q2", totalBatches, runtime);
            printQ2Results("pig");
        }
    }

    public static void loadPigQ3(int batchId, int totalBatches, long runtime) throws Exception {

        Properties props = new Properties();
        props.load(new FileInputStream("config/db.properties"));
        Connection conn = DriverManager.getConnection(
            props.getProperty("db.url"), props.getProperty("db.user"), props.getProperty("db.password"));

        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String outputDir = "/etl/output/pig/q3/batch_" + batchId;
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO results (" +
            "pipeline_name, query_name, run_id, batch_id, " +
            "log_date, log_hour, total_requests, request_count, error_rate, distinct_hosts, runtime_ms" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        int totalInserted = 0;

        for (FileStatus part : parts) {
            String name = part.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(part.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    String[] p2 = line.trim().split("\\t", 2);
                    if (p2.length < 2) continue;
                    String[] kp = p2[0].split("_");
                    if (kp.length < 2) continue;
                    String[] vp = p2[1].split("_");
                    if (vp.length < 4) continue;

                    ps.setString(1, "pig");
                    ps.setString(2, "Q3");
                    ps.setInt(3, 1);
                    ps.setInt(4, batchId);
                    ps.setString(5, kp[0]);
                    ps.setInt(6, Integer.parseInt(kp[1]));
                    ps.setInt(7, Integer.parseInt(vp[0])); // total requests
                    ps.setInt(8, Integer.parseInt(vp[1])); // error requests
                    ps.setDouble(9, Double.parseDouble(vp[2])); // error rate
                    ps.setInt(10, Integer.parseInt(vp[3])); // distinct hosts
                    ps.setLong(11, runtime);

                    ps.addBatch();
                    totalInserted++;
                } catch (Exception e) { /* skip bad lines */ }
            }
            br.close();
        }

        ps.executeBatch();
        fs.close(); ps.close(); conn.close();

        System.out.printf("Pig Q3 Batch %d/%d → %d rows inserted%n",
            batchId, totalBatches, totalInserted);

        if (batchId == totalBatches) {
            printBatchSummary("pig", "Q3", totalBatches, runtime);
            printQ3Results("pig");
        }
    }

    // ============================================================
    // RESULT PRINTERS  –  prints exactly the columns the assignment requires
    // ============================================================

    /** Q1: date, status_code, request_count, total_bytes */
    private static void printQ1Results(String pipeline) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("config/db.properties"));
        try (Connection conn = DriverManager.getConnection(
                props.getProperty("db.url"),
                props.getProperty("db.user"),
                props.getProperty("db.password"))) {

            String sql =
                "SELECT log_date, status_code, SUM(request_count) AS request_count, SUM(total_bytes) AS total_bytes " +
                "FROM results WHERE query_name='Q1' AND pipeline_name=? " +
                "GROUP BY log_date, status_code " +
                "ORDER BY log_date, status_code";

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, pipeline);
            ResultSet rs = ps.executeQuery();

            System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
            System.out.printf("║  Q1 Results [%s] – Daily Request Count by Status Code%n", pipeline.toUpperCase());
            System.out.println("╠══════════════════╦═════════════╦═══════════════╦═══════════════════╣");
            System.out.printf("║ %-16s ║ %-11s ║ %-13s ║ %-17s ║%n",
                "log_date", "status_code", "request_count", "total_bytes");
            System.out.println("╠══════════════════╬═════════════╬═══════════════╬═══════════════════╣");

            int rows = 0;
            while (rs.next()) {
                System.out.printf("║ %-16s ║ %-11d ║ %-13d ║ %-17d ║%n",
                    rs.getString("log_date"),
                    rs.getInt("status_code"),
                    rs.getLong("request_count"),
                    rs.getLong("total_bytes"));
                rows++;
            }
            System.out.println("╚══════════════════╩═════════════╩═══════════════╩═══════════════════╝");
            System.out.printf("  %d rows%n%n", rows);
        }
    }

    /** Q2: resource_path, request_count, total_bytes, distinct_host_count (Top 20) */
    private static void printQ2Results(String pipeline) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("config/db.properties"));
        try (Connection conn = DriverManager.getConnection(
                props.getProperty("db.url"),
                props.getProperty("db.user"),
                props.getProperty("db.password"))) {

            String sql =
                "SELECT resource_path, request_count, total_bytes, distinct_hosts " +
                "FROM results WHERE query_name='Q2' AND pipeline_name=? " +
                "ORDER BY request_count DESC LIMIT 20";

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, pipeline);
            ResultSet rs = ps.executeQuery();

            System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════╗");
            System.out.printf("║  Q2 Results [%s] – Top 20 Most Requested Resources%n", pipeline.toUpperCase());
            System.out.println("╠══════════════════════════════════════════════════╦═══════════════╦═══════════════════╦════════════════╣");
            System.out.printf("║ %-48s ║ %-13s ║ %-17s ║ %-14s ║%n",
                "resource_path", "request_count", "total_bytes", "distinct_hosts");
            System.out.println("╠══════════════════════════════════════════════════╬═══════════════╬═══════════════════╬════════════════╣");

            int rows = 0;
            while (rs.next()) {
                String path = rs.getString("resource_path");
                if (path.length() > 48) path = path.substring(0, 45) + "...";
                System.out.printf("║ %-48s ║ %-13d ║ %-17d ║ %-14d ║%n",
                    path,
                    rs.getLong("request_count"),
                    rs.getLong("total_bytes"),
                    rs.getInt("distinct_hosts"));
                rows++;
            }
            System.out.println("╚══════════════════════════════════════════════════╩═══════════════╩═══════════════════╩════════════════╝");
            System.out.printf("  %d rows%n%n", rows);
        }
    }

    /** Q3: log_date, log_hour, total_requests, error_count, error_rate, distinct_error_hosts */
    private static void printQ3Results(String pipeline) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("config/db.properties"));
        try (Connection conn = DriverManager.getConnection(
                props.getProperty("db.url"),
                props.getProperty("db.user"),
                props.getProperty("db.password"))) {

            String sql =
                "SELECT log_date, log_hour, SUM(total_requests) AS total_requests, " +
                "       SUM(request_count) AS error_requests, " +
                "       ROUND(AVG(error_rate)::numeric, 5) AS error_rate, " +
                "       SUM(distinct_hosts) AS distinct_error_hosts " +
                "FROM results WHERE query_name='Q3' AND pipeline_name=? " +
                "GROUP BY log_date, log_hour " +
                "ORDER BY log_date, log_hour";

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, pipeline);
            ResultSet rs = ps.executeQuery();

            System.out.println("\n╔════════════════════════════════════════════════════════════════════════════════════════════════╗");
            System.out.printf("║  Q3 Results [%s] – Hourly Error Rate Analysis%n", pipeline.toUpperCase());
            System.out.println("╠══════════════════╦══════════╦═══════════════╦════════════════╦════════════╦══════════════════════╣");
            System.out.printf("║ %-16s ║ %-8s ║ %-13s ║ %-14s ║ %-10s ║ %-20s ║%n",
                "log_date", "log_hour", "total_requests", "error_requests", "error_rate", "distinct_error_hosts");
            System.out.println("╠══════════════════╬══════════╬═══════════════╬════════════════╬════════════╬══════════════════════╣");

            int rows = 0;
            while (rs.next()) {
                System.out.printf("║ %-16s ║ %-8d ║ %-13d ║ %-14d ║ %-10s ║ %-20d ║%n",
                    rs.getString("log_date"),
                    rs.getInt("log_hour"),
                    rs.getLong("total_requests"),
                    rs.getLong("error_requests"),
                    rs.getString("error_rate"),
                    rs.getLong("distinct_error_hosts"));
                rows++;
            }
            System.out.println("╚══════════════════╩══════════╩═══════════════╩════════════════╩════════════╩══════════════════════╝");
            System.out.printf("  %d rows%n%n", rows);
        }
    }

    // ============================================================
    // BATCH SUMMARY
    // ============================================================

    private static void printBatchSummary(String pipeline, String query,
                                           int totalBatches, long totalRuntimeMs) {
        long totalInputRecords = (long) totalBatches * BatchSplitter.BATCH_SIZE;
        System.out.println("\n========================================");
        System.out.println("  Pipeline      : " + pipeline.toUpperCase());
        System.out.println("  Query         : " + query);
        System.out.println("  Total batches : " + totalBatches);
        System.out.println("  Batch size    : " + BatchSplitter.BATCH_SIZE + " input records");
        System.out.printf ("  Avg batch size: %.0f input records%n",
                           (double) totalInputRecords / totalBatches);
        System.out.printf ("  Total runtime : %.2f sec%n", totalRuntimeMs / 1000.0);
        System.out.println("========================================\n");
    }

    // ============================================================
    // Legacy main (kept for backward compatibility)
    // ============================================================
    public static void main(String[] args) throws Exception {
        System.out.println("Use Main.java to run pipelines.");
    }
}