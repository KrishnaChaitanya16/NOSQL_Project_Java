package com.etl.pipeline;

import com.etl.db.ResultLoader;
import com.etl.util.BatchSplitter;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MongoPipeline implements Pipeline {

    private static final Pattern LOG_PATTERN = Pattern.compile(
            "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[([^\\]]+)\\]\\s+\"(.*)\"\\s+(\\d{3})\\s+(\\S+)$"
    );

    private static final Pattern REQUEST_PATTERN = Pattern.compile(
            "^(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?$"
    );

    @Override
    public void runQ1() throws Exception {
        System.out.println("Running MongoDB Q1...");
        List<String> batches = BatchSplitter.split();
        long totalRuntime = 0;
        long totalMalformed = 0;

        try (MongoClient mongoClient = new MongoClient("localhost", 27017)) {
            MongoDatabase db = mongoClient.getDatabase("nasa_logs");

            for (int batchId = 1; batchId <= batches.size(); batchId++) {
                String inputPath = batches.get(batchId - 1);
                System.out.println("\n--- Mongo Q1 Batch " + batchId + "/" + batches.size() + " ---");

                long start = System.currentTimeMillis();
                MongoCollection<Document> collection = db.getCollection("q1_batch");
                collection.drop();

                long malformed = parseAndInsert(inputPath, collection, 1);
                totalMalformed += malformed;

                AggregateIterable<Document> results = collection.aggregate(Arrays.asList(
                        new Document("$group", new Document("_id",
                                new Document("log_date", "$log_date").append("status_code", "$status_code"))
                                .append("request_count", new Document("$sum", 1))
                                .append("total_bytes", new Document("$sum", "$bytes")))
                ));

                long batchRuntime = System.currentTimeMillis() - start;
                totalRuntime += batchRuntime;

                ResultLoader.loadMongoQ1(batchId, batches.size(), totalRuntime, results);
            }
        }

        System.out.println("\nMongo Q1 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q1: " + totalMalformed);
    }

    @Override
    public void runQ2() throws Exception {
        System.out.println("Running MongoDB Q2...");
        List<String> batches = BatchSplitter.split();
        long totalRuntime = 0;
        long totalMalformed = 0;

        try (MongoClient mongoClient = new MongoClient("localhost", 27017)) {
            MongoDatabase db = mongoClient.getDatabase("nasa_logs");
            MongoCollection<Document> globalMergeCollection = db.getCollection("q2_global");
            globalMergeCollection.drop();

            // Stage 1: Batch Aggregation
            for (int batchId = 1; batchId <= batches.size(); batchId++) {
                String inputPath = batches.get(batchId - 1);
                System.out.println("\n--- Mongo Q2 Batch " + batchId + "/" + batches.size() + " ---");

                long start = System.currentTimeMillis();
                MongoCollection<Document> collection = db.getCollection("q2_batch");
                collection.drop();

                long malformed = parseAndInsert(inputPath, collection, 2);
                totalMalformed += malformed;

                AggregateIterable<Document> batchResults = collection.aggregate(Arrays.asList(
                        new Document("$group", new Document("_id",
                                new Document("path", "$path").append("host", "$host"))
                                .append("req_count", new Document("$sum", 1))
                                .append("bytes", new Document("$sum", "$bytes"))),
                        new Document("$group", new Document("_id", "$_id.path")
                                .append("req_count", new Document("$sum", "$req_count"))
                                .append("bytes", new Document("$sum", "$bytes"))
                                .append("hosts", new Document("$addToSet", "$_id.host")))
                ));

                List<Document> toInsert = new ArrayList<>();
                for (Document doc : batchResults) {
                    doc.put("path", doc.get("_id"));
                    doc.remove("_id");
                    toInsert.add(doc);
                }
                if (!toInsert.isEmpty()) {
                    globalMergeCollection.insertMany(toInsert);
                }

                totalRuntime += System.currentTimeMillis() - start;
            }

            System.out.println("\n--- Mongo Q2 Stage-2 Global Merge ---");
            long mergeStart = System.currentTimeMillis();
            
            AggregateIterable<Document> finalResults = globalMergeCollection.aggregate(Arrays.asList(
                    new Document("$group", new Document("_id", "$path")
                            .append("req_count", new Document("$sum", "$req_count"))
                            .append("bytes", new Document("$sum", "$bytes"))
                            .append("host_arrays", new Document("$push", "$hosts"))),
                    new Document("$project", new Document("req_count", 1)
                            .append("bytes", 1)
                            .append("distinct_hosts", new Document("$size", 
                                    new Document("$reduce", new Document("input", "$host_arrays")
                                            .append("initialValue", java.util.Collections.emptyList())
                                            .append("in", new Document("$setUnion", Arrays.asList("$$value", "$$this")))
                                    )
                            ))),
                    new Document("$sort", new Document("req_count", -1)),
                    new Document("$limit", 20)
            ));

            totalRuntime += System.currentTimeMillis() - mergeStart;
            ResultLoader.loadMongoQ2(batches.size(), batches.size(), totalRuntime, finalResults);
        }

        System.out.println("\nMongo Q2 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q2: " + totalMalformed);
    }

    @Override
    public void runQ3() throws Exception {
        System.out.println("Running MongoDB Q3...");
        List<String> batches = BatchSplitter.split();
        long totalRuntime = 0;
        long totalMalformed = 0;

        try (MongoClient mongoClient = new MongoClient("localhost", 27017)) {
            MongoDatabase db = mongoClient.getDatabase("nasa_logs");

            for (int batchId = 1; batchId <= batches.size(); batchId++) {
                String inputPath = batches.get(batchId - 1);
                System.out.println("\n--- Mongo Q3 Batch " + batchId + "/" + batches.size() + " ---");

                long start = System.currentTimeMillis();
                MongoCollection<Document> collection = db.getCollection("q3_batch");
                collection.drop();

                long malformed = parseAndInsert(inputPath, collection, 3);
                totalMalformed += malformed;

                AggregateIterable<Document> results = collection.aggregate(Arrays.asList(
                        new Document("$group", new Document("_id",
                                new Document("log_date", "$log_date").append("log_hour", "$log_hour").append("host", "$host"))
                                .append("total_requests", new Document("$sum", 1))
                                .append("error_requests", new Document("$sum", new Document("$cond", Arrays.asList(new Document("$and", Arrays.asList(new Document("$gte", Arrays.asList("$status_code", 400)), new Document("$lte", Arrays.asList("$status_code", 599)))), 1, 0))))),
                        new Document("$group", new Document("_id",
                                new Document("log_date", "$_id.log_date").append("log_hour", "$_id.log_hour"))
                                .append("total_requests", new Document("$sum", "$total_requests"))
                                .append("error_requests", new Document("$sum", "$error_requests"))
                                .append("distinct_hosts", new Document("$sum", new Document("$cond", Arrays.asList(new Document("$gt", Arrays.asList("$error_requests", 0)), 1, 0))))),
                        new Document("$project", new Document("total_requests", 1)
                                .append("error_requests", 1)
                                .append("distinct_hosts", 1)
                                .append("error_rate", new Document("$divide", Arrays.asList("$error_requests", "$total_requests"))))
                ));

                long batchRuntime = System.currentTimeMillis() - start;
                totalRuntime += batchRuntime;

                ResultLoader.loadMongoQ3(batchId, batches.size(), totalRuntime, results);
            }
        }

        System.out.println("\nMongo Q3 completed. Total runtime: " + totalRuntime + " ms");
        System.out.println("==> TOTAL MALFORMED RECORDS FOR Q3: " + totalMalformed);
    }

    private long parseAndInsert(String hdfsPath, MongoCollection<Document> collection, int queryNum) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsPath))));

        String line;
        long malformed = 0;
        List<Document> batch = new ArrayList<>();
        final int INSERT_BATCH_SIZE = 10000;

        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;

            Matcher m = LOG_PATTERN.matcher(line);
            if (!m.find()) {
                malformed++;
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
                try { bytes = Long.parseLong(bytesStr); } catch (Exception ignored) {}
            }

            Document doc = new Document();

            if (queryNum == 1) {
                doc.append("log_date", date).append("status_code", status).append("bytes", bytes);
            } else if (queryNum == 2) {
                Matcher req = REQUEST_PATTERN.matcher(request);
                if (!req.matches()) continue;
                String method = req.group(1);
                String path = req.group(2);
                if (!method.equals("GET") && !method.equals("POST") && !method.equals("HEAD")) continue;
                try { path = URLDecoder.decode(path, "UTF-8"); } catch (Exception ignored) {}
                path = path.replaceAll("[^\\x20-\\x7E]", "").trim();
                if (!path.startsWith("/")) continue;
                doc.append("host", host).append("path", path).append("bytes", bytes);
            } else if (queryNum == 3) {
                String[] dtParts = timestamp.split(":");
                if (dtParts.length < 2) continue;
                String hour = dtParts[1];
                doc.append("log_date", date).append("log_hour", Integer.parseInt(hour)).append("status_code", status).append("host", host);
            }

            batch.add(doc);
            if (batch.size() >= INSERT_BATCH_SIZE) {
                collection.insertMany(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            collection.insertMany(batch);
        }
        br.close();
        return malformed;
    }
}
