package dev.datarepair;

import dev.datarepair.db.Database;
import org.junit.jupiter.api.Assumptions;

import javax.sql.DataSource;
import java.util.UUID;

public final class IntegrationEnvironment implements AutoCloseable {
    private final String schema;
    private final DataSource admin;
    private final DataSource scoped;

    private IntegrationEnvironment(String schema, DataSource admin, DataSource scoped) {
        this.schema = schema;
        this.admin = admin;
        this.scoped = scoped;
    }

    public static IntegrationEnvironment create() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("repair.integration"),
                "run with -Drepair.integration=true after docker compose up");
        String base = System.getProperty("repair.jdbc",
                "jdbc:postgresql://localhost:54329/repairs");
        String user = System.getProperty("repair.db.user", "repairs");
        String password = System.getProperty("repair.db.password", "repairs");
        String schema = "it_" + UUID.randomUUID().toString().replace("-", "");
        DataSource admin = Database.dataSource(base, user, password);
        try (var c = admin.getConnection(); var ps = c.prepareStatement("CREATE SCHEMA " + schema)) {
            ps.execute();
        }
        String separator = base.contains("?") ? "&" : "?";
        DataSource scoped = Database.dataSource(base + separator + "currentSchema=" + schema, user, password);
        Database.initialize(scoped);
        return new IntegrationEnvironment(schema, admin, scoped);
    }

    public DataSource dataSource() {
        return scoped;
    }

    public static String kafka() {
        return System.getProperty("repair.kafka", "localhost:19092");
    }

    @Override public void close() throws Exception {
        if (scoped instanceof AutoCloseable closeable) closeable.close();
        try (var c = admin.getConnection(); var ps = c.prepareStatement("DROP SCHEMA " + schema + " CASCADE")) {
            ps.execute();
        }
        if (admin instanceof AutoCloseable closeable) closeable.close();
    }
}
