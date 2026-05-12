package com.etl.db;

import java.io.*;
import java.sql.*;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ResultLoader {

    // ============================================================
    // SHARED HELPERS — write to run_metadata, batch_metadata,
    //                   and malformed_record_summary tables
    // ============================================================

    /** Get a JDBC connection from db.properties */
    private static Connection getConnection() throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("config/db.properties"));
        return DriverManager.getConnection(
            props.getProperty("db.url"),
            props.getProperty("db.user"),
            props.getProperty("db.password"));
    }

    /** Insert a row into run_metadata and return the generated run_id */
    public static int createRun(String pipelineName) throws Exception {
        Connection conn = getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO run_metadata (pipeline_name) VALUES (?)",
            Statement.RETURN_GENERATED_KEYS);
        ps.setString(1, pipelineName);
        ps.executeUpdate();

        ResultSet rs = ps.getGeneratedKeys();
        rs.next();
        int runId = rs.getInt(1);

        rs.close(); ps.close(); conn.close();
        System.out.println("Created run_id = " + runId + " for pipeline: " + pipelineName);
        return runId;
    }

    /** Insert a row into batch_metadata */
    public static void saveBatchMeta(int runId, int batchId, String batchLabel,
                                      long batchSize, double avgBatchSize,
                                      long recordsProcessed, long runtimeMs) throws Exception {
        Connection conn = getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO batch_metadata " +
            "(run_id, batch_id, batch_label, batch_size, average_batch_size, records_processed, runtime_ms) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)");
        ps.setInt(1, runId);
        ps.setInt(2, batchId);
        ps.setString(3, batchLabel);
        ps.setLong(4, batchSize);
        ps.setDouble(5, avgBatchSize);
        ps.setLong(6, recordsProcessed);
        ps.setLong(7, runtimeMs);
        ps.executeUpdate();
        ps.close(); conn.close();
    }

    /** Insert a row into malformed_record_summary */
    public static void saveMalformed(int runId, int batchId, String queryName,
                                      long malformedCount) throws Exception {
        Connection conn = getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO malformed_record_summary " +
            "(run_id, batch_id, query_name, malformed_record_count) " +
            "VALUES (?, ?, ?, ?)");
        ps.setInt(1, runId);
        ps.setInt(2, batchId);
        ps.setString(3, queryName);
        ps.setLong(4, malformedCount);
        ps.executeUpdate();
        ps.close(); conn.close();
    }

    // ============================================================
    // MAPREDUCE LOADERS — insert into query_results
    // ============================================================

    public static void loadQ1(int runId, int batchId, int totalBatches, long runtime) throws Exception {

        Connection conn = getConnection();
        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String outputDir = "/etl/output/mapreduce/q1/batch_" + batchId;
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO query_results " +
            "(run_id, batch_id, query_name, log_date, status_code, request_count, total_bytes) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)");

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

                    ps.setInt(1, runId);
                    ps.setInt(2, batchId);
                    ps.setString(3, "Q1");
                    ps.setString(4, date);
                    ps.setInt(5, status);
                    ps.setInt(6, count);
                    ps.setLong(7, bytes);

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
            printQ1Results("mapreduce", runId);
        }
    }

    public static void loadQ2(int runId, int batchId, int totalBatches, long runtime) throws Exception {

        Connection conn = getConnection();
        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String outputDir = "/etl/output/mapreduce/q2/final";
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO query_results " +
            "(run_id, batch_id, query_name, resource_path, request_count, total_bytes, distinct_hosts) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)");

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

                    ps.setInt(1, runId);
                    ps.setInt(2, batchId);
                    ps.setString(3, "Q2");
                    ps.setString(4, p2[0]);
                    ps.setInt(5, Integer.parseInt(vals[0]));
                    ps.setLong(6, Long.parseLong(vals[1]));
                    ps.setInt(7, Integer.parseInt(vals[2]));

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
            printQ2Results("mapreduce", runId);
        }
    }

    public static void loadQ3(int runId, int batchId, int totalBatches, long runtime) throws Exception {

        Connection conn = getConnection();
        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String outputDir = "/etl/output/mapreduce/q3/batch_" + batchId;
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO query_results " +
            "(run_id, batch_id, query_name, log_date, log_hour, " +
            " error_request_count, total_requests, error_rate, distinct_error_hosts) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

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
                    if (vp.length < 4) continue;

                    ps.setInt(1, runId);
                    ps.setInt(2, batchId);
                    ps.setString(3, "Q3");
                    ps.setString(4, kp[0]);
                    ps.setInt(5, Integer.parseInt(kp[1]));
                    ps.setInt(6, Integer.parseInt(vp[1]));    // error request count
                    ps.setInt(7, Integer.parseInt(vp[0]));    // total requests
                    ps.setDouble(8, Double.parseDouble(vp[2])); // error rate
                    ps.setInt(9, Integer.parseInt(vp[3]));    // distinct error hosts

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
            printQ3Results("mapreduce", runId);
        }
    }

    // ============================================================
    // PIG LOADERS
    // ============================================================

    public static void loadPigQ1(int runId, int batchId, int totalBatches, long runtime) throws Exception {
        Connection conn = getConnection();
        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String outputDir = "/etl/output/pig/q1/batch_" + batchId;
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO query_results " +
            "(run_id, batch_id, query_name, log_date, status_code, request_count, total_bytes) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)");

        int totalInserted = 0;

        for (FileStatus part : parts) {
            String name = part.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(part.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    String[] p2 = line.split("\\t");
                    if (p2.length < 2) continue;
                    String[] key = p2[0].split("_");
                    String[] val = p2[1].split("_");

                    ps.setInt(1, runId);
                    ps.setInt(2, batchId);
                    ps.setString(3, "Q1");
                    ps.setString(4, key[0]);
                    ps.setInt(5, Integer.parseInt(key[1]));
                    ps.setInt(6, Integer.parseInt(val[0]));
                    ps.setLong(7, Long.parseLong(val[1]));

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
            printQ1Results("pig", runId);
        }
    }

    public static void loadPigQ2(int runId, int batchId, int totalBatches, long runtime) throws Exception {
        Connection conn = getConnection();
        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String outputDir = "/etl/output/pig/q2/final";
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO query_results " +
            "(run_id, batch_id, query_name, resource_path, request_count, total_bytes, distinct_hosts) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)");

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
                    String[] vals = p2[1].split("_");
                    if (vals.length < 3) continue;

                    ps.setInt(1, runId);
                    ps.setInt(2, batchId);
                    ps.setString(3, "Q2");
                    ps.setString(4, p2[0]);
                    ps.setInt(5, Integer.parseInt(vals[0]));
                    ps.setLong(6, Long.parseLong(vals[1]));
                    ps.setInt(7, Integer.parseInt(vals[2]));

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
            printQ2Results("pig", runId);
        }
    }

    public static void loadPigQ3(int runId, int batchId, int totalBatches, long runtime) throws Exception {
        Connection conn = getConnection();
        Configuration conf = new Configuration();
        FileSystem    fs   = FileSystem.get(conf);

        String outputDir = "/etl/output/pig/q3/batch_" + batchId;
        FileStatus[] parts = fs.listStatus(new Path(outputDir));

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO query_results " +
            "(run_id, batch_id, query_name, log_date, log_hour, " +
            " error_request_count, total_requests, error_rate, distinct_error_hosts) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

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

                    ps.setInt(1, runId);
                    ps.setInt(2, batchId);
                    ps.setString(3, "Q3");
                    ps.setString(4, kp[0]);
                    ps.setInt(5, Integer.parseInt(kp[1]));
                    ps.setLong(6, Long.parseLong(vp[1]));
                    ps.setLong(7, Long.parseLong(vp[0]));
                    ps.setDouble(8, Double.parseDouble(vp[2]));
                    ps.setInt(9, Integer.parseInt(vp[3]));

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
            printQ3Results("pig", runId);
        }
    }

    // ============================================================
    // MONGO LOADERS
    // ============================================================

    public static void loadMongoQ1(int runId, int batchId, int totalBatches, long runtime, Iterable<org.bson.Document> docs) throws Exception {
        Connection conn = getConnection();

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO query_results " +
            "(run_id, batch_id, query_name, log_date, status_code, request_count, total_bytes) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)");

        int totalInserted = 0;
        for (org.bson.Document doc : docs) {
            org.bson.Document id = (org.bson.Document) doc.get("_id");
            ps.setInt(1, runId);
            ps.setInt(2, batchId);
            ps.setString(3, "Q1");
            ps.setString(4, id.getString("log_date"));
            ps.setInt(5, ((Number) id.get("status_code")).intValue());
            ps.setLong(6, ((Number) doc.get("request_count")).longValue());
            ps.setLong(7, ((Number) doc.get("total_bytes")).longValue());

            ps.addBatch();
            totalInserted++;
        }

        ps.executeBatch();
        ps.close(); conn.close();

        System.out.printf("Mongo Q1 Batch %d/%d → %d rows inserted%n",
            batchId, totalBatches, totalInserted);

        if (batchId == totalBatches) {
            printQ1Results("mongo", runId);
        }
    }

    public static void loadMongoQ2(int runId, int batchId, int totalBatches, long runtime, Iterable<org.bson.Document> docs) throws Exception {
        Connection conn = getConnection();

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO query_results " +
            "(run_id, batch_id, query_name, resource_path, request_count, total_bytes, distinct_hosts) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)");

        int totalInserted = 0;
        for (org.bson.Document doc : docs) {
            ps.setInt(1, runId);
            ps.setInt(2, batchId);
            ps.setString(3, "Q2");
            ps.setString(4, doc.getString("_id"));
            ps.setLong(5, ((Number) doc.get("req_count")).longValue());
            ps.setLong(6, ((Number) doc.get("bytes")).longValue());
            ps.setInt(7, ((Number) doc.get("distinct_hosts")).intValue());

            ps.addBatch();
            totalInserted++;
        }

        ps.executeBatch();
        ps.close(); conn.close();

        System.out.printf("Mongo Q2 Batch %d/%d → %d rows inserted%n",
            batchId, totalBatches, totalInserted);

        if (batchId == totalBatches) {
            printQ2Results("mongo", runId);
        }
    }

    public static void loadMongoQ3(int runId, int batchId, int totalBatches, long runtime, Iterable<org.bson.Document> docs) throws Exception {
        Connection conn = getConnection();

        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO query_results " +
            "(run_id, batch_id, query_name, log_date, log_hour, " +
            " error_request_count, total_requests, error_rate, distinct_error_hosts) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

        int totalInserted = 0;
        for (org.bson.Document doc : docs) {
            org.bson.Document id = (org.bson.Document) doc.get("_id");
            ps.setInt(1, runId);
            ps.setInt(2, batchId);
            ps.setString(3, "Q3");
            ps.setString(4, id.getString("log_date"));
            ps.setInt(5, ((Number) id.get("log_hour")).intValue());
            ps.setLong(6, ((Number) doc.get("error_requests")).longValue());
            ps.setLong(7, ((Number) doc.get("total_requests")).longValue());
            ps.setDouble(8, ((Number) doc.get("error_rate")).doubleValue());
            ps.setInt(9, ((Number) doc.get("distinct_hosts")).intValue());

            ps.addBatch();
            totalInserted++;
        }

        ps.executeBatch();
        ps.close(); conn.close();

        System.out.printf("Mongo Q3 Batch %d/%d → %d rows inserted%n",
            batchId, totalBatches, totalInserted);

        if (batchId == totalBatches) {
            printQ3Results("mongo", runId);
        }
    }

    // ============================================================
    // RESULT PRINTERS  –  prints exactly the columns the assignment requires
    // ============================================================

    /** Q1: date, status_code, request_count, total_bytes */
    private static void printQ1Results(String pipeline, int runId) throws Exception {
        try (Connection conn = getConnection()) {

            String sql =
                "SELECT log_date, status_code, SUM(request_count) AS request_count, SUM(total_bytes) AS total_bytes " +
                "FROM query_results WHERE query_name='Q1' AND run_id=? " +
                "GROUP BY log_date, status_code " +
                "ORDER BY log_date, status_code";

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, runId);
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
    private static void printQ2Results(String pipeline, int runId) throws Exception {
        try (Connection conn = getConnection()) {

            String sql =
                "SELECT resource_path, request_count, total_bytes, distinct_hosts " +
                "FROM query_results WHERE query_name='Q2' AND run_id=? " +
                "ORDER BY request_count DESC LIMIT 20";

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, runId);
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
    private static void printQ3Results(String pipeline, int runId) throws Exception {
        try (Connection conn = getConnection()) {

            String sql =
                "SELECT log_date, log_hour, SUM(total_requests) AS total_requests, " +
                "       SUM(error_request_count) AS error_requests, " +
                "       ROUND(AVG(error_rate)::numeric, 5) AS error_rate, " +
                "       SUM(distinct_error_hosts) AS distinct_error_hosts " +
                "FROM query_results WHERE query_name='Q3' AND run_id=? " +
                "GROUP BY log_date, log_hour " +
                "ORDER BY log_date, log_hour";

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, runId);
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
    // BATCH SUMMARY — reads from batch_metadata table
    // ============================================================

    public static void printBatchSummary(String pipeline, int runId) throws Exception {
        try (Connection conn = getConnection()) {
            String sql =
                "SELECT batch_id, batch_label, batch_size, average_batch_size, " +
                "       records_processed, runtime_ms " +
                "FROM batch_metadata WHERE run_id=? ORDER BY batch_id";

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, runId);
            ResultSet rs = ps.executeQuery();

            System.out.println("\n========================================");
            System.out.println("  Pipeline : " + pipeline.toUpperCase());
            System.out.println("  Run ID   : " + runId);
            System.out.println("----------------------------------------");

            long totalRuntime = 0;
            while (rs.next()) {
                System.out.printf("  Batch %d [%s]: size=%d, processed=%d, avg=%.0f, runtime=%d ms%n",
                    rs.getInt("batch_id"),
                    rs.getString("batch_label"),
                    rs.getLong("batch_size"),
                    rs.getLong("records_processed"),
                    rs.getDouble("average_batch_size"),
                    rs.getLong("runtime_ms"));
                totalRuntime += rs.getLong("runtime_ms");
            }
            System.out.printf("  Total runtime : %.2f sec%n", totalRuntime / 1000.0);
            System.out.println("========================================\n");
        }
    }

    // ============================================================
    // Legacy main (kept for backward compatibility)
    // ============================================================
    public static void main(String[] args) throws Exception {
        System.out.println("Use Main.java to run pipelines.");
    }
}