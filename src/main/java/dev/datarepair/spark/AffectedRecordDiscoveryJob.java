package dev.datarepair.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;

/**
 * Distributed affected-record discovery. The output is immutable Parquet that
 * can be reviewed, counted, sampled, and then imported as a repair plan.
 */
public final class AffectedRecordDiscoveryJob {
    private AffectedRecordDiscoveryJob() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5 && args.length != 6) {
            throw new IllegalArgumentException(
                    "usage: <jdbc-url> <user> <password> <bad-config-version> <output-path> [metrics-json]");
        }
        SparkSession spark = SparkSession.builder()
                .appName("affected-record-discovery")
                .getOrCreate();
        try {
            var query = """
                    (SELECT DISTINCT p.payment_id,p.amount_cents,p.fee_bps,p.fee_cents,
                       p.captured_cents,p.refunded_cents,p.merchant_net_cents,p.row_version,
                       l.code_version,l.config_version
                     FROM payments p JOIN record_lineage l ON l.record_id=p.payment_id
                     WHERE l.config_version='%s') affected
                    """.formatted(args[3].replace("'", "''"));
            var affected = spark.read()
                    .format("jdbc")
                    .option("url", args[0])
                    .option("dbtable", query)
                    .option("user", args[1])
                    .option("password", args[2])
                    .option("driver", "org.postgresql.Driver")
                    .option("partitionColumn", "amount_cents")
                    .option("lowerBound", "0")
                    .option("upperBound", "500000")
                    .option("numPartitions", "8")
                    .load();
            affected.cache();
            long discoveryStarted = System.nanoTime();
            long count = affected.count();
            long discoveryNanos = System.nanoTime() - discoveryStarted;
            long parquetStarted = System.nanoTime();
            affected.write().mode(SaveMode.Overwrite).parquet(args[4]);
            long parquetNanos = System.nanoTime() - parquetStarted;
            double discoveryRps = count / (discoveryNanos / 1_000_000_000.0);
            System.out.println("AFFECTED_RECORDS=" + count);
            System.out.println("DISCOVERY_MILLIS=" + discoveryNanos / 1_000_000);
            System.out.println("DISCOVERY_RECORDS_PER_SECOND=" + discoveryRps);
            System.out.println("PARQUET_WRITE_MILLIS=" + parquetNanos / 1_000_000);
            if (args.length == 6) {
                var metrics = new LinkedHashMap<String, Object>();
                metrics.put("generatedAt", Instant.now().toString());
                metrics.put("affectedRecords", count);
                metrics.put("discoveryMillis", discoveryNanos / 1_000_000);
                metrics.put("discoveryRecordsPerSecond", discoveryRps);
                metrics.put("parquetWriteMillis", parquetNanos / 1_000_000);
                metrics.put("partitions", affected.rdd().getNumPartitions());
                metrics.put("passed", count > 0);
                Files.writeString(Path.of(args[5]),
                        new ObjectMapper().writerWithDefaultPrettyPrinter()
                                .writeValueAsString(metrics));
            }
        } finally {
            spark.stop();
        }
    }
}
