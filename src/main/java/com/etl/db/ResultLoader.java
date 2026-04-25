package com.etl.db;

import java.io.*;
import java.sql.*;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ResultLoader {

    public static void loadQ1(long runtime) throws Exception {

    // -------- Load DB Config --------
    Properties props = new Properties();
    props.load(new FileInputStream("config/db.properties"));

    String url = props.getProperty("db.url");
    String user = props.getProperty("db.user");
    String password = props.getProperty("db.password");

    Connection conn = DriverManager.getConnection(url, user, password);

    // -------- HDFS Setup --------
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    Path path = new Path("/etl/output/mapreduce/q1/part-r-00000");

    BufferedReader br = new BufferedReader(
            new InputStreamReader(fs.open(path))
    );

    // -------- Batching Variables --------
    int batchSize = 100;
    int batchId = 1;
    int countInBatch = 0;
    int totalRecords = 0;

    // -------- Prepare Statement (OUTSIDE LOOP) --------
    PreparedStatement ps = conn.prepareStatement(
        "INSERT INTO results (" +
        "pipeline_name, query_name, run_id, batch_id, " +
        "log_date, log_hour, status_code, resource_path, " +
        "request_count, total_requests, total_bytes, distinct_hosts, error_rate, runtime_ms" +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );

    String line;

    while ((line = br.readLine()) != null) {

        String[] parts = line.split("\\s+");

        String[] key = parts[0].split("_");
        String[] val = parts[1].split("_");

        String date = key[0];
        int status = Integer.parseInt(key[1]);

        int count = Integer.parseInt(val[0]);
        long bytes = Long.parseLong(val[1]);

        // -------- Set Values --------
        ps.setString(1, "mapreduce");
        ps.setString(2, "Q1");
        ps.setInt(3, 1); // run_id
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

        countInBatch++;
        totalRecords++;

        // -------- Execute Batch --------
        if (countInBatch == batchSize) {
            ps.executeBatch();
            batchId++;
            countInBatch = 0;
        }
    }

    // -------- Execute Remaining --------
    if (countInBatch > 0) {
        ps.executeBatch();
    }

    // -------- Metrics --------
    double avgBatchSize = (double) totalRecords / batchId;

    System.out.println("Total Records: " + totalRecords);
    System.out.println("Total Batches: " + batchId);
    System.out.println("Avg Batch Size: " + avgBatchSize);

    // -------- Close Resources --------
    br.close();
    fs.close();
    ps.close();
    conn.close();

    System.out.println("Data loaded into PostgreSQL successfully (from HDFS).");
}

public static void loadQ2(long runtime) throws Exception {

    // -------- Load DB Config --------
    Properties props = new Properties();
    props.load(new FileInputStream("config/db.properties"));

    String url = props.getProperty("db.url");
    String user = props.getProperty("db.user");
    String password = props.getProperty("db.password");

    Connection conn = DriverManager.getConnection(url, user, password);

    // -------- HDFS Setup --------
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    Path path = new Path("/etl/output/mapreduce/q2/part-r-00000");

    BufferedReader br = new BufferedReader(
            new InputStreamReader(fs.open(path))
    );

    // -------- Prepare Statement --------
    PreparedStatement ps = conn.prepareStatement(
        "INSERT INTO results (" +
        "pipeline_name, query_name, run_id, batch_id, " +
        "resource_path, request_count, total_bytes, distinct_hosts, runtime_ms" +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );

    int batchId = 1;
    int countInBatch = 0;
    int batchSize = 100;
    int totalInserted = 0;

    String line;

    while ((line = br.readLine()) != null) {

        try {
           
            String[] parts = line.trim().split("\\s+", 2);

            if (parts.length < 2) continue;

            String resource = parts[0];
            String valuePart = parts[1];

            String[] vals = valuePart.split("_");
            if (vals.length < 3) continue;

            int requestCount = Integer.parseInt(vals[0]);
            long totalBytes = Long.parseLong(vals[1]);
            int distinctHosts = Integer.parseInt(vals[2]);

            // -------- Set Values --------
            ps.setString(1, "mapreduce");
            ps.setString(2, "Q2");
            ps.setInt(3, 1);
            ps.setInt(4, batchId);

            ps.setString(5, resource);
            ps.setInt(6, requestCount);
            ps.setLong(7, totalBytes);
            ps.setInt(8, distinctHosts);

            ps.setLong(9, runtime);

            ps.addBatch();

            countInBatch++;
            totalInserted++;

            // -------- Execute Batch --------
            if (countInBatch == batchSize) {
                ps.executeBatch();
                batchId++;
                countInBatch = 0;
            }

        } catch (Exception e) {
            // optional debug:
            // System.out.println("Skipped line: " + line);
        }
    }

    // -------- Execute remaining --------
    if (countInBatch > 0) {
        ps.executeBatch();
    }

    br.close();
    fs.close();
    ps.close();
    conn.close();

    System.out.println("Q2 loaded into DB successfully.");
    System.out.println("Total inserted rows: " + totalInserted);
}
    public static void main(String[] args) throws Exception {

    if (args.length == 0) {
        System.out.println("Provide query: Q1 or Q2");
        return;
    }

    if (args[0].equalsIgnoreCase("Q1")) {
        loadQ1(0);
    } else if (args[0].equalsIgnoreCase("Q2")) {
        loadQ2(0);
    } else {
        System.out.println("Invalid query");
    }
}
}