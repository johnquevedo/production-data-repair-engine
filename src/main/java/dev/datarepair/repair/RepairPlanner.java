package dev.datarepair.repair;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.datarepair.db.Database;
import dev.datarepair.model.PaymentSnapshot;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

public final class RepairPlanner {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final DataSource dataSource;

    public RepairPlanner(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Plan preview(String badConfigVersion, int targetFeeBps, int partitions) throws Exception {
        try (var connection = dataSource.getConnection();
             var ps = connection.prepareStatement("""
                     SELECT DISTINCT p.payment_id,p.amount_cents,p.fee_bps,p.fee_cents,p.captured_cents,
                       p.refunded_cents,p.merchant_net_cents,p.status,p.code_version,p.config_version,p.row_version
                     FROM payments p JOIN record_lineage l ON l.record_id=p.payment_id
                     WHERE l.config_version=? AND p.fee_bps<>?
                     ORDER BY p.payment_id
                     """)) {
            ps.setString(1, badConfigVersion);
            ps.setInt(2, targetFeeBps);
            var items = new ArrayList<PlannedItem>();
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var before = new PaymentSnapshot(rs.getObject(1, UUID.class), rs.getLong(2),
                            rs.getInt(3), rs.getLong(4), rs.getLong(5), rs.getLong(6),
                            rs.getLong(7), rs.getString(8), rs.getString(9), rs.getString(10),
                            rs.getLong(11));
                    var after = before.withFee(targetFeeBps);
                    int partition = Math.floorMod(before.paymentId().hashCode(), partitions);
                    items.add(new PlannedItem(before, after, partition));
                }
            }
            items.sort(Comparator.comparing(i -> i.before().paymentId()));
            return new Plan(badConfigVersion, targetFeeBps, hash(items), List.copyOf(items));
        }
    }

    public UUID persist(Plan plan, boolean dryRun) throws Exception {
        UUID jobId = UUID.randomUUID();
        Database.transaction(dataSource, connection -> {
            try (PreparedStatement job = connection.prepareStatement("""
                    INSERT INTO repair_jobs(job_id,bad_config_version,target_fee_bps,plan_hash,status,dry_run,affected_count)
                    VALUES (?,?,?,?,'PLANNED',?,?)
                    """)) {
                job.setObject(1, jobId);
                job.setString(2, plan.badConfigVersion());
                job.setInt(3, plan.targetFeeBps());
                job.setString(4, plan.planHash());
                job.setBoolean(5, dryRun);
                job.setLong(6, plan.items().size());
                job.executeUpdate();
            }
            try (PreparedStatement item = connection.prepareStatement("""
                    INSERT INTO repair_items(job_id,payment_id,repair_key,partition_id,expected_row_version,
                      before_image,after_image,state) VALUES (?,?,?,?,?,?::jsonb,?::jsonb,'PENDING')
                    """)) {
                for (PlannedItem planned : plan.items()) {
                    item.setObject(1, jobId);
                    item.setObject(2, planned.before().paymentId());
                    item.setString(3, sha256(jobId + ":" + planned.before().paymentId()));
                    item.setInt(4, planned.partition());
                    item.setLong(5, planned.before().rowVersion());
                    item.setString(6, JSON.writeValueAsString(planned.before()));
                    item.setString(7, JSON.writeValueAsString(planned.after()));
                    item.addBatch();
                }
                item.executeBatch();
            }
            audit(connection, jobId, null, "PLAN_CREATED", null,
                    JSON.writeValueAsString(java.util.Map.of("planHash", plan.planHash(),
                            "affectedCount", plan.items().size(), "dryRun", dryRun)));
            return null;
        });
        return jobId;
    }

    static void audit(java.sql.Connection connection, UUID jobId, UUID paymentId,
                      String action, String worker, String details) throws Exception {
        try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO audit_log(job_id,payment_id,action,worker_id,details)
                VALUES (?,?,?,?,?::jsonb)
                """)) {
            ps.setObject(1, jobId);
            ps.setObject(2, paymentId);
            ps.setString(3, action);
            ps.setString(4, worker);
            ps.setString(5, details);
            ps.executeUpdate();
        }
    }

    private static String hash(List<PlannedItem> items) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        for (PlannedItem item : items) {
            digest.update(JSON.writeValueAsBytes(item));
            digest.update((byte) '\n');
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    static String sha256(String value) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    public record PlannedItem(PaymentSnapshot before, PaymentSnapshot after, int partition) {}
    public record Plan(String badConfigVersion, int targetFeeBps, String planHash,
                       List<PlannedItem> items) {}
}
