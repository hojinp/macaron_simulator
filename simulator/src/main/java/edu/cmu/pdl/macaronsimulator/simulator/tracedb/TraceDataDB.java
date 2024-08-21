package edu.cmu.pdl.macaronsimulator.simulator.tracedb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.cmu.pdl.macaronsimulator.common.TaskParallelRunner;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.AccessInfo;
import edu.cmu.pdl.macaronsimulator.simulator.message.TraceQueryTypeMap;

public class TraceDataDB {
    private static final String URL_PREFIX = "jdbc:sqlite:";
    private static boolean ready = false;

    // The path to the trace file and the database file.
    public static String traceFilePath = null;
    public static String dbFilePath = null;

    // The total number of accesses in the given time range. The access history array stores the access history in the
    // given time range.
    public static int totalAccessCount;
    public static AccessInfo accessHistory[];
    private static int accessHistorySize = (int) 1e8;

    // Give consecutive requests in the ascending time order.
    private static Connection seqConn = null;
    private static PreparedStatement seqStmt = null;
    private static ResultSet seqRs = null;
    public static boolean seqReady = false, seqDone = false;
    public static long seqTimestamp, seqObjectSize;
    public static int seqObjectId, seqOpType; // 0 for PUT, 1 for GET, and 2 for DELETE

    public TraceDataDB() {
    }

    public static void initialize() {
        if (dbFilePath == null)
            throw new RuntimeException("dbFilePath is not set");

        try {
            Files.createDirectories(new File(dbFilePath).getParentFile().toPath());
            ready = databaseExists() ? true : false;
            if (!ready) {
                createTable();
                storeTraceToDB();
            }
            initializeAccessHistory();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Initialize the access history array.
     */
    private static void initializeAccessHistory() {
        Logger.getGlobal().info("Initializing access history array");
        accessHistory = new AccessInfo[accessHistorySize];
        TaskParallelRunner.run((threadId, threadLength) -> {
            for (int i = threadId; i < accessHistorySize; i += threadLength) {
                accessHistory[i] = new AccessInfo();
            }
        });
        Logger.getGlobal().info("Done initializing access history array");
    }

    /**
     * Check if the database file exists.
     * 
     * @return true if the database file exists, false otherwise.
     */
    private static boolean databaseExists() {
        File dbFile = new File(dbFilePath);
        return dbFile.exists();
    }

    /**
     * Create the table in the database. The table name is trace_data and it has 4 columns: timestamp, object_id,
     * op_type, object_size.
     */
    private static void createTable() {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + dbFilePath);
                Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS trace_data (timestamp integer, object_id integer, "
                    + "op_type integer, object_size integer)";
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Store the trace data to the database. The trace file is a csv file with 4 columns: timestamp, op_type, object_id,
     * object_size. The timestamp is in milliseconds. The op_type is 0 for GET, 1 for PUT, and 2 for DELETE.
     * 
     * @param traceFilePath The path to the trace file.
     */
    private static void storeTraceToDB() {
        if (ready) {
            Logger.getGlobal().info("TraceDataDB is already ready");
            return;
        }

        Logger.getGlobal().info("Preparing TraceDataDB: load requests from trace file");
        int batchSize = (int) 1e8, batchIdx = 0, batchCnt = 0;
        long[] timestamps = new long[batchSize];
        int[] opTypes = new int[batchSize];
        int[] objectIds = new int[batchSize];
        long[] objectSizes = new long[batchSize];
        try (BufferedReader br = new BufferedReader(new FileReader(traceFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] fields = line.strip().split(",");
                timestamps[batchIdx] = Long.parseLong(fields[0]) * 1000L; // convert ms to us
                opTypes[batchIdx] = Integer.parseInt(fields[1]);
                objectIds[batchIdx] = Integer.parseInt(fields[2]);
                objectSizes[batchIdx] = opTypes[batchIdx] == 2 ? -1L : Long.parseLong(fields[3]);
                batchIdx++;

                if (batchIdx == batchSize) {
                    Logger.getGlobal()
                            .info("Preparing TraceDataDB: # of objects processed: " + (++batchCnt * batchSize));
                    insertBulkData(timestamps, objectIds, opTypes, objectSizes, batchIdx);
                    batchIdx = 0;
                }
            }

            if (batchIdx > 0) {
                Logger.getGlobal()
                        .info("Preparing TraceDataDB: # of objects processed: " + (batchCnt * batchSize + batchIdx));
                insertBulkData(timestamps, objectIds, opTypes, objectSizes, batchIdx);
            }
        } catch (NumberFormatException | IOException e) {
            throw new RuntimeException(e);
        }

        createIndex();
        ready = true;
        Logger.getGlobal().info("Done preparing TraceDataDB, including creating index");
    }

    /**
     * Insert a batch of data to the database. The batch size is batchIdx.
     * 
     * @param timestamps The timestamps of the batch of data.
     * @param objectIds The object ids of the batch of data.
     * @param opTypes The operation types of the batch of data.
     * @param objectSizes The object sizes of the batch of data.
     * @param batchIdx The batch size.
     */
    private static void insertBulkData(long[] timestamps, int[] objectIds, int[] opTypes, long[] objectSizes,
            int batchIdx) {
        String sql = "INSERT INTO trace_data(timestamp, object_id, op_type, object_size) VALUES(?,?,?,?)";
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + dbFilePath);
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false); // Start transaction
            for (int i = 0; i < batchIdx; i++) {
                pstmt.setLong(1, timestamps[i]);
                pstmt.setInt(2, objectIds[i]);
                pstmt.setInt(3, opTypes[i]);
                pstmt.setLong(4, objectSizes[i]);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            conn.commit(); // Commit transaction
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Process the data in the given time range. The time range is [startTime, endTime). The data is stored in the
     * accessHistory array.
     * 
     * @param startTime The start time of the time range.
     * @param endTime The end time of the time range.
     */
    public static void loadDataInTimeRange(long startTime, long endTime) {
        if (!ready) {
            throw new RuntimeException("TraceDataDB is not ready");
        }
        if (!timestampIndexExists()) {
            createIndex();
        }

        String countSql = "SELECT COUNT(*) FROM trace_data WHERE timestamp BETWEEN ? AND ?";
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + dbFilePath);
                PreparedStatement stmt = conn.prepareStatement(countSql)) {
            stmt.setLong(1, startTime);
            stmt.setLong(2, endTime);
            ResultSet countRs = stmt.executeQuery();
            totalAccessCount = countRs.next() ? countRs.getInt(1) : 0;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }

        // Check if accessHistorySize is enough
        if (totalAccessCount > accessHistorySize) {
            Logger.getGlobal().warning("totalAccessCount > accessHistorySize");
            accessHistorySize = totalAccessCount;
            initializeAccessHistory();
        }

        String readSql = "SELECT * FROM trace_data WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp ASC";
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + dbFilePath)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("PRAGMA journal_mode = OFF;");
            }

            try (PreparedStatement stmt = conn.prepareStatement(readSql)) {
                stmt.setLong(1, startTime);
                stmt.setLong(2, endTime);

                int idx = 0;
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    accessHistory[idx].ts = rs.getLong("timestamp");
                    accessHistory[idx].objectId = rs.getInt("object_id");
                    accessHistory[idx].qType = TraceQueryTypeMap.get(rs.getInt("op_type"));
                    accessHistory[idx].val = rs.getLong("object_size");
                    idx++;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Prepare to start Application's request queue.
     */
    public static void prepareNextRequest() {
        if (!ready)
            throw new RuntimeException("TraceDataDB is not ready");

        try {
            if (seqConn != null) {
                seqRs = null;
                seqConn.close();
            }

            Logger.getGlobal().info("Preparing TraceDataDB: load requests from database");
            seqConn = DriverManager.getConnection(URL_PREFIX + dbFilePath);
            try (Statement stmt = seqConn.createStatement()) {
                stmt.execute("PRAGMA journal_mode = OFF;");
            }
            String sqlQuery = "SELECT * FROM trace_data ORDER BY timestamp ASC";
            seqStmt = seqConn.prepareStatement(sqlQuery);
            seqReady = true;
            seqDone = false;
            Logger.getGlobal().info("Done preparing TraceDataDB");
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * For Application: give requests in the ascending time order.
     * 
     * @return each request in the ascending time order.
     */
    public static boolean getNextRequest() {
        if (!ready || !seqReady)
            throw new RuntimeException("TraceDataDB is not ready or Application is not ready");

        if (seqDone)
            return false;

        try {
            if (seqRs == null)
                seqRs = seqStmt.executeQuery();

            if (seqRs.next()) {
                seqTimestamp = seqRs.getLong("timestamp");
                seqObjectId = seqRs.getInt("object_id");
                seqOpType = seqRs.getInt("op_type");
                seqObjectSize = seqRs.getLong("object_size");
                return true;
            } else {
                Logger.getGlobal().info("No more requests from TraceDataDB.");
                seqDone = true;
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Get the maximum object id in the trace.
     * 
     * @return the maximum object id in the trace.
     */
    public static int getMaxObjectId() {
        if (!ready)
            throw new RuntimeException("TraceDataDB is not ready");

        String sql = "SELECT MAX(object_id) FROM trace_data";
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + dbFilePath);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Check if the timestamp index exists.
     */
    private static boolean timestampIndexExists() {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + dbFilePath);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT name, tbl_name FROM sqlite_master WHERE type = 'index';")) {
            while (rs.next()) {
                String indexName = rs.getString("name");
                String tableName = rs.getString("tbl_name");
                if (indexName.equals("idx_timestamp") && tableName.equals("trace_data"))
                    return true;
            }
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Get the maximum timestamp in the trace.
     * @return the maximum timestamp in the trace.
     */
    public static long getMaxTimestamp() {
        if (!ready)
            throw new RuntimeException("TraceDataDB is not ready");

        String sql = "SELECT MAX(timestamp) FROM trace_data";
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + dbFilePath);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Create the timestamp index.
     */
    private static void createIndex() {
        Logger.getGlobal().info("Creating index on timestamp");
        String sql = "CREATE INDEX IF NOT EXISTS idx_timestamp ON trace_data (timestamp);";
        try (Connection conn = DriverManager.getConnection(URL_PREFIX + dbFilePath);
                Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA journal_mode = OFF;");
            stmt.execute("PRAGMA temp_store_directory = '" + (new File(dbFilePath)).getParent() + "';");
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
        Logger.getGlobal().info("Done creating index on timestamp");
    }

    public static void main(String[] args) throws ParseException {
        // How to run this class using maven: mvn exec:java -Dexec.mainClass="edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB" -Dexec.args="-t /path/to/tracefile -d /path/to/dbfile"
        Options options = new Options();
        options.addOption("t", "tracefile", true, "input tracefile");
        options.addOption("d", "tracedbfile", true, "TraceDB's file path");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        String traceFilePath = cmd.getOptionValue("tracefile");
        String dbFilePath = cmd.getOptionValue("dbfile");
        if (!new File(traceFilePath).exists()) {
            throw new RuntimeException("traceFilePath does not exist");
        }

        TraceDataDB.traceFilePath = traceFilePath;
        TraceDataDB.dbFilePath = dbFilePath;
        TraceDataDB traceDataDB = new TraceDataDB();

        getNextRequestTest(traceFilePath);
    }

    /**
     * Test function that checks if the first 10 lines of a file match the original values of the trace, specifically 
     * when the file is named IBMTrace000.csv.
     */
    private static void getNextRequestTest(String traceFilePath) {
        if (!traceFilePath.endsWith("IBMTrace000.csv")) {
            Logger.getGlobal().info("This test is for IBMTrace000.csv only.");
            return;
        }

        long[] timestamps = new long[] { 0, 13480, 15537, 37548, 37601, 52262, 65806, 66515, 70360, 70371 };
        int[] opTypes = new int[] { 0, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
        int[] objectIds = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        long[] objectSizes = new long[] { 1056, 1168, 528, 23872, 11376, 8432, 21568, 211504, 10432, 14512 };
        TraceDataDB.prepareNextRequest();
        for (int i = 0; i < timestamps.length; i++) {
            if (!TraceDataDB.getNextRequest())
                throw new RuntimeException("getNextRequest() returns false");
            if (timestamps[i] * (int) 1e6 != TraceDataDB.seqTimestamp)
                throw new RuntimeException("timestamps[" + i + "] != TraceDataDB.appTimestamp: "
                        + (timestamps[i] * (int) 1e6) + " != " + TraceDataDB.seqTimestamp);
            if (opTypes[i] != TraceDataDB.seqOpType)
                throw new RuntimeException(
                        "opTypes[" + i + "] != TraceDataDB.appOpType: " + opTypes[i] + " != " + TraceDataDB.seqOpType);
            if (objectIds[i] != TraceDataDB.seqObjectId)
                throw new RuntimeException("objectIds[" + i + "] != TraceDataDB.appObjectId: " + objectIds[i] + " != "
                        + TraceDataDB.seqObjectId);
            if (objectSizes[i] != TraceDataDB.seqObjectSize)
                throw new RuntimeException("objectSizes[" + i + "] != TraceDataDB.appObjectSize: " + objectSizes[i]
                        + " != " + TraceDataDB.seqObjectSize);
        }
        Logger.getGlobal().info("getNextRequestTest passed");
    }
}
