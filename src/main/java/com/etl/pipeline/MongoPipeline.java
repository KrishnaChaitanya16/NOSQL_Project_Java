package com.etl.pipeline;

import com.etl.db.ResultLoader;
import scripts.mongo.MongoQ1;
import scripts.mongo.MongoQ2;
import scripts.mongo.MongoQ3;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MongoPipeline implements Pipeline {

    private static final String DB_NAME = "nasa_logs";

    private static final Pattern LOG_PATTERN = Pattern.compile(
            "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+\"(.*)\"\\s+(\\d{3})\\s+(\\S+)$"
    );

    private static final Pattern REQUEST_PATTERN = Pattern.compile(
            "^(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?$"
    );

    private static class ParseStats {
        long recordsProcessed;
        long malformed;
    }

    // ----------------------------------------------------------------
    // Q1 – Daily request count + total bytes per status code
    // ----------------------------------------------------------------
    @Override
    public void runQ1(int runId, List<String[]> batches) throws Exception {

        System.out.println("Running MongoDB Q1...");

        long totalRuntime = 0;
        long totalMalformed = 0;

        try (MongoClient mongoClient = new MongoClient("localhost", 27017)) {
            MongoDatabase db = mongoClient.getDatabase(DB_NAME);

            for (int batchId = 1; batchId <= batches.size(); batchId++) {
                String[] batchInfo = batches.get(batchId - 1);
                String inputPath = batchInfo[0];
                String batchLabel = batchInfo[1];
                String inputCollection = "q1_batch";
                String outputCollection = "q1_results";

                System.out.println("\n--- Mongo Q1 Batch " + batchId + "/" + batches.size() + " [" + batchLabel + "] ---");
                System.out.println("  Input : " + inputPath);
                System.out.println("  Output: " + outputCollection);

                long start = System.currentTimeMillis();

                MongoCollection<Document> collection = db.getCollection(inputCollection);
                collection.drop();

                ParseStats stats = parseAndInsert(inputPath, collection, 1);
                totalMalformed += stats.malformed;

                MongoQ1.runAggregation(collection, outputCollection);

                long batchRuntime = System.currentTimeMillis() - start;
                totalRuntime += batchRuntime;

                long batchSizeBytes = getHdfsPathSize(inputPath);
                ResultLoader.saveBatchMeta(runId, batchId, batchLabel, batchSizeBytes, batchSizeBytes,
                        stats.recordsProcessed, batchRuntime);
                ResultLoader.saveMalformed(runId, batchId, "Q1", stats.malformed);

                MongoCollection<Document> results = db.getCollection(outputCollection);
                ResultLoader.loadMongoQ1(runId, batchId, batches.size(), totalRuntime, results.find());
            }
        }

        System.out.println("\nMongo Q1 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q1: " + totalMalformed);
        ResultLoader.printBatchSummary("mongo", runId);
    }

    // ----------------------------------------------------------------
    // Q2 – Top 20 most-requested resources (TWO-STAGE)
    // ----------------------------------------------------------------
    @Override
    public void runQ2(int runId, List<String[]> batches) throws Exception {

        System.out.println("Running MongoDB Q2 (Two-Stage)...");

        long totalRuntime = 0;
        long totalMalformed = 0;

        String stage1Global = "q2_stage1_global";
        String stage2Output = "q2_results";

        try (MongoClient mongoClient = new MongoClient("localhost", 27017)) {
            MongoDatabase db = mongoClient.getDatabase(DB_NAME);
            db.getCollection(stage1Global).drop();

            // ---- Stage 1: one batch at a time ----
            for (int batchId = 1; batchId <= batches.size(); batchId++) {
                String[] batchInfo = batches.get(batchId - 1);
                String inputPath = batchInfo[0];
                String batchLabel = batchInfo[1];
                String inputCollection = "q2_batch";

                System.out.println("\n--- Mongo Q2 Stage-1 Batch " + batchId + "/" + batches.size() + " [" + batchLabel + "] ---");
                System.out.println("  Input : " + inputPath);
                System.out.println("  Output: " + stage1Global + " (merge)");

                long start = System.currentTimeMillis();

                MongoCollection<Document> collection = db.getCollection(inputCollection);
                collection.drop();

                ParseStats stats = parseAndInsert(inputPath, collection, 2);
                totalMalformed += stats.malformed;

                MongoQ2.runStage1(collection, db, stage1Global, batchId);

                long batchRuntime = System.currentTimeMillis() - start;
                totalRuntime += batchRuntime;

                long batchSizeBytes = getHdfsPathSize(inputPath);
                ResultLoader.saveBatchMeta(runId, batchId, batchLabel, batchSizeBytes, batchSizeBytes,
                        stats.recordsProcessed, batchRuntime);
                ResultLoader.saveMalformed(runId, batchId, "Q2", stats.malformed);
            }

            // ---- Stage 2: global merge ----
            System.out.println("\n--- Mongo Q2 Stage-2 Global Merge ---");
            long mergeStart = System.currentTimeMillis();

            MongoQ2.runStage2(db, stage1Global, stage2Output);

            totalRuntime += System.currentTimeMillis() - mergeStart;

            MongoCollection<Document> results = db.getCollection(stage2Output);
            ResultLoader.loadMongoQ2(runId, batches.size(), batches.size(), totalRuntime, results.find());
        }

        System.out.println("\nMongo Q2 completed (Two-Stage). Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q2: " + totalMalformed);
        ResultLoader.printBatchSummary("mongo", runId);
    }

    // ----------------------------------------------------------------
    // Q3 – Error rate per hour
    // ----------------------------------------------------------------
    @Override
    public void runQ3(int runId, List<String[]> batches) throws Exception {

        System.out.println("Running MongoDB Q3...");

        long totalRuntime = 0;
        long totalMalformed = 0;

        try (MongoClient mongoClient = new MongoClient("localhost", 27017)) {
            MongoDatabase db = mongoClient.getDatabase(DB_NAME);

            for (int batchId = 1; batchId <= batches.size(); batchId++) {
                String[] batchInfo = batches.get(batchId - 1);
                String inputPath = batchInfo[0];
                String batchLabel = batchInfo[1];
                String inputCollection = "q3_batch";
                String outputCollection = "q3_results";

                System.out.println("\n--- Mongo Q3 Batch " + batchId + "/" + batches.size() + " [" + batchLabel + "] ---");
                System.out.println("  Input : " + inputPath);
                System.out.println("  Output: " + outputCollection);

                long start = System.currentTimeMillis();

                MongoCollection<Document> collection = db.getCollection(inputCollection);
                collection.drop();

                ParseStats stats = parseAndInsert(inputPath, collection, 3);
                totalMalformed += stats.malformed;

                MongoQ3.runAggregation(collection, outputCollection);

                long batchRuntime = System.currentTimeMillis() - start;
                totalRuntime += batchRuntime;

                long batchSizeBytes = getHdfsPathSize(inputPath);
                ResultLoader.saveBatchMeta(runId, batchId, batchLabel, batchSizeBytes, batchSizeBytes,
                        stats.recordsProcessed, batchRuntime);
                ResultLoader.saveMalformed(runId, batchId, "Q3", stats.malformed);

                MongoCollection<Document> results = db.getCollection(outputCollection);
                ResultLoader.loadMongoQ3(runId, batchId, batches.size(), totalRuntime, results.find());
            }
        }

        System.out.println("\nMongo Q3 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q3: " + totalMalformed);
        ResultLoader.printBatchSummary("mongo", runId);
    }

    private ParseStats parseAndInsert(String hdfsPath, MongoCollection<Document> collection, int queryNum) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        ParseStats stats = new ParseStats();
        List<Document> batch = new ArrayList<>();
        final int insertBatchSize = 10000;

        for (Path path : listHdfsFiles(fs, new Path(hdfsPath))) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                stats.recordsProcessed++;

                Matcher m = LOG_PATTERN.matcher(line);
                if (!m.find()) {
                    stats.malformed++;
                    continue;
                }

                String host = m.group(1);
                String timestamp = m.group(2);
                String request = m.group(3);
                String statusStr = m.group(4);
                String bytesStr = m.group(5);

                String date = timestamp.split(":")[0];
                int status = 0;
                try { status = Integer.parseInt(statusStr); } catch (Exception ignored) {}

                long bytes = 0;
                if (!bytesStr.equals("-")) {
                    try { bytes = Long.parseLong(bytesStr); if (bytes < 0) bytes = 0; } catch (Exception ignored) { bytes = 0; }
                }

                Document doc = new Document();

                if (queryNum == 1) {
                    doc.append("log_date", date).append("status_code", status).append("bytes", bytes);
                } else if (queryNum == 2) {
                    Matcher req = REQUEST_PATTERN.matcher(request);
                    if (!req.matches()) continue;
                    String method = req.group(1);
                    String reqPath = req.group(2);
                    if (!method.equals("GET") && !method.equals("POST") && !method.equals("HEAD")) continue;
                    try { reqPath = URLDecoder.decode(reqPath, "UTF-8"); } catch (Exception ignored) {}
                    reqPath = reqPath.replaceAll("[^\\x20-\\x7E]", "").trim();
                    if (!reqPath.startsWith("/")) continue;
                    doc.append("host", host).append("path", reqPath).append("bytes", bytes);
                } else if (queryNum == 3) {
                    String[] dtParts = timestamp.split(":");
                    if (dtParts.length < 2) continue;
                    String hour = dtParts[1];
                    doc.append("log_date", date)
                            .append("log_hour", Integer.parseInt(hour))
                            .append("status_code", status)
                            .append("host", host);
                }

                batch.add(doc);
                if (batch.size() >= insertBatchSize) {
                    collection.insertMany(batch);
                    batch.clear();
                }
            }
            br.close();
        }

        if (!batch.isEmpty()) {
            collection.insertMany(batch);
        }

        fs.close();
        return stats;
    }

    private long getHdfsPathSize(String path) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            long size = sumHdfsPathSize(fs, new Path(path));
            fs.close();
            return size;
        } catch (Exception e) {
            return 0;
        }
    }

    private long sumHdfsPathSize(FileSystem fs, Path path) throws IOException {
        FileStatus status = fs.getFileStatus(path);
        if (!status.isDirectory()) {
            return status.getLen();
        }
        long total = 0;
        for (FileStatus child : fs.listStatus(path)) {
            total += sumHdfsPathSize(fs, child.getPath());
        }
        return total;
    }

    private List<Path> listHdfsFiles(FileSystem fs, Path path) throws IOException {
        List<Path> files = new ArrayList<>();
        collectHdfsFiles(fs, path, files);
        return files;
    }

    private void collectHdfsFiles(FileSystem fs, Path path, List<Path> out) throws IOException {
        FileStatus status = fs.getFileStatus(path);
        if (!status.isDirectory()) {
            out.add(path);
            return;
        }

        for (FileStatus child : fs.listStatus(path)) {
            String name = child.getPath().getName();
            if (name.startsWith("_") || name.startsWith(".")) continue;
            if (child.isDirectory()) {
                collectHdfsFiles(fs, child.getPath(), out);
            } else {
                out.add(child.getPath());
            }
        }
    }

}
