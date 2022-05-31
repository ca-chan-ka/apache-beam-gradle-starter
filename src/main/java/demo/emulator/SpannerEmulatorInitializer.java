package demo.emulator;

import java.nio.file.*;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.cloud.spanner.*;

import demo.main.Ship;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SpannerEmulatorInitializer {

    private final String emulatorHost;
    private final String projectId;
    private final String instanceId;
    private final String databaseId;
    private final List<String> queries;

    private SpannerEmulatorInitializer(
            String emulatorHost,
            String projectId,
            String instanceId,
            String databaseId,
            List<String> queries
    ) {
        this.emulatorHost = emulatorHost;
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.databaseId = databaseId;
        this.queries = queries;
    }

    /** Initialize the Spanner Emulator */
    private void run() throws Exception {
        SpannerOptions options = SpannerOptions
                .newBuilder()
                .setProjectId(projectId)
                .setEmulatorHost(emulatorHost)
                .build();
        Spanner spanner = options.getService();
        createInstance(spanner);
        createDatabase(spanner);
        readData(spanner);
    }

    private void readData(Spanner spanner) {
        log.info("Reading data from Spanner emulator...");
        DatabaseClient client = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
        ResultSet rowCount = client.readOnlyTransaction().executeQuery(Statement.of("SELECT count(*) FROM Ship"));
        while (rowCount.next()) {
            log.info("Row count: " + rowCount.getLong(0));
        }

        ResultSet result = client.readOnlyTransaction().executeQuery(Statement.of("SELECT * FROM Ship"));
        while (result.next()) {
            log.info(Ship.of(result).toString());
        }
    }

    private void createDatabase(Spanner spanner) throws Exception {
        log.info("Creating database in Spanner emulator...");
        DatabaseAdminClient client = spanner.getDatabaseAdminClient();
        try {
            client.getDatabase(instanceId, databaseId);
        } catch (DatabaseNotFoundException e) {
            Database database = client.newDatabaseBuilder(DatabaseId.of(projectId, instanceId, databaseId)).build();
            client.createDatabase(database, queries).get();
        }

    }

    private void createInstance(Spanner spanner) throws SpannerException, InterruptedException {
        log.info("Creating instance in Spanner emulator...");
        InstanceAdminClient client = spanner.getInstanceAdminClient();
        InstanceInfo info = InstanceInfo
                .newBuilder(InstanceId.of(projectId, instanceId))
                .build();
        try {
            client.createInstance(info).get();
        } catch (ExecutionException e) {
            log.warn("Suppressed exeption when creating instance: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        log.info("Initializing Spanner Emulator");

        String query = Files.readString(Path.of("ddl/ship.sql"));
        String projectId = "demo-project";
        String intanceId = "demo-instance";
        String databaseId = "demo-database";
        String emulatorHost = "localhost:9010";

        new SpannerEmulatorInitializer(emulatorHost, projectId, intanceId, databaseId, List.of(query)).run();
    }
}
