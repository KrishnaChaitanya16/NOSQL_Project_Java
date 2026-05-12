package scripts.mongo;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;

/**
 * Q3 – Hourly HTTP error rate.
 *
 * Aggregation (two-phase group):
 *   $group  by {log_date, log_hour, host} → total_requests, error_requests (4xx/5xx)
 *   $group  by {log_date, log_hour}       → total_requests, error_requests, distinct_hosts (hosts with errors)
 *   $project with error_rate = error_requests / total_requests
 *   $out    → output collection
 */
public class MongoQ3 {

    private MongoQ3() {}

    /**
     * Runs the Q3 aggregation pipeline on the given input collection
     * and writes results to the specified output collection.
     */
    public static void runAggregation(MongoCollection<Document> inputCollection, String outputCollection) {
        List<Document> pipeline = Arrays.asList(
                Document.parse("{ $group: { _id: { log_date: '$log_date', log_hour: '$log_hour', host: '$host' }, "
                        + "total_requests: { $sum: 1 }, "
                        + "error_requests: { $sum: { $cond: [ "
                        + "{ $and: [ { $gte: ['$status_code', 400] }, { $lte: ['$status_code', 599] } ] }, 1, 0 ] } } } }"),
                Document.parse("{ $group: { _id: { log_date: '$_id.log_date', log_hour: '$_id.log_hour' }, "
                        + "total_requests: { $sum: '$total_requests' }, "
                        + "error_requests: { $sum: '$error_requests' }, "
                        + "distinct_hosts: { $sum: { $cond: [{ $gt: ['$error_requests', 0] }, 1, 0] } } } }"),
                Document.parse("{ $project: { total_requests: 1, error_requests: 1, distinct_hosts: 1, "
                        + "error_rate: { $cond: [ { $eq: ['$total_requests', 0] }, 0, "
                        + "{ $divide: ['$error_requests', '$total_requests'] } ] } } }"),
                Document.parse("{ $out: '" + outputCollection + "' }")
        );
        inputCollection.aggregate(pipeline).allowDiskUse(true).first();
    }
}
