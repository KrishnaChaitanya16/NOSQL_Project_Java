package scripts.mongo;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;

/**
 * Q1 – Daily request count + total bytes per status code.
 *
 * Aggregation:
 *   $group  { _id: {log_date, status_code}, request_count: sum(1), total_bytes: sum(bytes) }
 *   $out    → output collection
 */
public class MongoQ1 {

    private MongoQ1() {}

    /**
     * Runs the Q1 aggregation pipeline on the given input collection
     * and writes results to the specified output collection.
     */
    public static void runAggregation(MongoCollection<Document> inputCollection, String outputCollection) {
        List<Document> pipeline = Arrays.asList(
                Document.parse("{ $group: { _id: { log_date: '$log_date', status_code: '$status_code' }, "
                        + "request_count: { $sum: 1 }, total_bytes: { $sum: '$bytes' } } }"),
                Document.parse("{ $out: '" + outputCollection + "' }")
        );
        inputCollection.aggregate(pipeline).allowDiskUse(true).first();
    }
}
