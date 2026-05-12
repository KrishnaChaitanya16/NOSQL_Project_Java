package scripts.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;

/**
 * Q2 – Top 20 most-requested resources (TWO-STAGE).
 *
 * Stage 1 (per-batch):
 *   $group  by {path, host} → req_count, bytes   (one doc per unique path+host)
 *   $project with batch_id literal
 *   $merge  into stage1 global collection
 *
 * Stage 2 (global):
 *   $group  by {path, host} across batches → sum req_count, bytes   (dedup hosts globally)
 *   $group  by path → sum req_count, bytes, distinct_hosts = count
 *   $sort   by req_count desc
 *   $limit  20
 *   $out    → output collection
 *
 * This approach mirrors MapReduce's HashSet-based host deduplication exactly:
 * each unique (path, host) pair is stored as a separate document, and distinct
 * hosts are counted by grouping — no $addToSet/$setUnion arrays involved.
 */
public class MongoQ2 {

    private MongoQ2() {}

    /**
     * Stage 1: Per-batch aggregation that stores one document per unique
     * (path, host) pair into the global stage-1 collection.
     */
    public static void runStage1(MongoCollection<Document> inputCollection,
                                 MongoDatabase db,
                                 String stage1Global,
                                 int batchId) {

        List<Document> stage1Pipeline = Arrays.asList(
                Document.parse("{ $group: { _id: { path: '$path', host: '$host' }, "
                        + "req_count: { $sum: 1 }, bytes: { $sum: '$bytes' } } }"),
                Document.parse("{ $project: { _id: 0, path: '$_id.path', host: '$_id.host', "
                        + "batch_id: { $literal: " + batchId + " }, "
                        + "req_count: 1, bytes: 1 } }"),
                Document.parse("{ $match: { path: { $ne: null }, host: { $ne: null } } }"),
                Document.parse("{ $merge: { into: '" + stage1Global + "', "
                        + "on: ['path', 'host', 'batch_id'], "
                        + "whenMatched: 'replace', whenNotMatched: 'insert' } }")
        );

        db.getCollection(stage1Global).createIndex(
                Document.parse("{ path: 1, host: 1, batch_id: 1 }"),
                new IndexOptions().unique(true));

        inputCollection.aggregate(stage1Pipeline).allowDiskUse(true).first();
    }

    /**
     * Stage 2: Global merge across all batch stage-1 results.
     * First groups by (path, host) to deduplicate hosts across batches,
     * then groups by path to count distinct hosts — mirroring MapReduce's
     * HashSet deduplication exactly.
     */
    public static void runStage2(MongoDatabase db,
                                 String stage1Global,
                                 String stage2Output) {

        List<Document> stage2Pipeline = Arrays.asList(
                // Deduplicate hosts across batches: merge (path, host) pairs
                Document.parse("{ $group: { _id: { path: '$path', host: '$host' }, "
                        + "req_count: { $sum: '$req_count' }, bytes: { $sum: '$bytes' } } }"),
                // Count distinct hosts per path
                Document.parse("{ $group: { _id: '$_id.path', "
                        + "req_count: { $sum: '$req_count' }, bytes: { $sum: '$bytes' }, "
                        + "distinct_hosts: { $sum: 1 } } }"),
                Document.parse("{ $sort: { req_count: -1 } }"),
                Document.parse("{ $limit: 20 }"),
                Document.parse("{ $out: '" + stage2Output + "' }")
        );

        db.getCollection(stage1Global).aggregate(stage2Pipeline).allowDiskUse(true).first();
    }
}
