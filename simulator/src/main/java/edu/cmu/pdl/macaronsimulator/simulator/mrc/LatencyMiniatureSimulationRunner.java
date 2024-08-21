package edu.cmu.pdl.macaronsimulator.simulator.mrc;

import edu.cmu.pdl.macaronsimulator.common.CustomLoggerGetter;
import edu.cmu.pdl.macaronsimulator.simulator.latency.LatencyGenerator;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfoStore;
import edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class LatencyMiniatureSimulationRunner {
    public static void main(String[] args) throws IOException {
        Logger logger = CustomLoggerGetter.getCustomLogger();
        logger.info("[LatencyMiniatureSimluationRunner] Start running");

        // Options
        Options options = new Options();
        options.addRequiredOption("f", "TraceFilename", true, "Path to the trace filename");
        options.addRequiredOption("o", "OutputDirname", true, "Output directory name");
        options.addRequiredOption("s", "SamplingRatio", true, "Sampling ratio of MiniatureSimulation");
        options.addRequiredOption("c", "MiniCacheCount", true, "The number of miniature caches");
        options.addRequiredOption("u", "MiniCacheSizeUnitMB", true, "Unit size of the miniature caches");
        options.addRequiredOption("k", "MaxObjectId", true, "Maximum object id of the given trace");
        options.addRequiredOption("p", "tracedbfilePath", true, "Path to the trace DB file");
        options.addRequiredOption("m", "ProfileIntervalMinutes", true, "Profile interval in minutes");
        options.addRequiredOption("r", "InterRegion", true, "Inter-region latency");
        options.addOption("t", "DisableConsiderTiming", false, "Disable considering timing (default: false)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        String traceFilename = cmd.getOptionValue("f");
        String outDirname = cmd.getOptionValue("o");
        double samplingRatio = Double.parseDouble(cmd.getOptionValue("s"));
        int miniCacheCount = Integer.parseInt(cmd.getOptionValue("c"));
        int cacheSizeUnitMB = Integer.parseInt(cmd.getOptionValue("u"));
        int maxObjectId = Integer.parseInt(cmd.getOptionValue("k"));
        if (cmd.hasOption("t")) {
            LatencyMiniatureSimulation.CONSIDER_TIMING = false;
        }

        TraceDataDB.traceFilePath = traceFilename;
        TraceDataDB.dbFilePath = cmd.getOptionValue("p");
        TraceDataDB.initialize();

        if (!Files.exists(Paths.get(traceFilename)) || Files.isDirectory(Paths.get(outDirname)))
            throw new RuntimeException("\nTracefile " + traceFilename + "\nOutputDirname: " + outDirname);
        try {
            Files.createDirectories(Paths.get(outDirname));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ProfileInfoStore.LOG_DIRNAME = outDirname;

        long oscSizeByte = 100L * 1024L * 1024L * 1024L * 1024L;
        String interRegion = cmd.getOptionValue("r");
        assert interRegion.equals("use1-usw1") || interRegion.equals("use1-euc1") : "Unexpected inter-region latency: " + interRegion;
        LatencyGenerator.setInterRegions(interRegion);
        LatencyGenerator.setup(true);
        LatencyMiniatureSimulation sim = new LatencyMiniatureSimulation(samplingRatio, miniCacheCount, cacheSizeUnitMB, maxObjectId, oscSizeByte);

        long profileInterval = Long.parseLong(cmd.getOptionValue("m")) * 60L * 1000000L;
        long maxTimestamp = TraceDataDB.getMaxTimestamp();
        int numIntervals = (int) (maxTimestamp / profileInterval) + 1;
        long prevProfileStartTs = 0L;
        for (int i = 0; i < numIntervals; i++) {
            TraceDataDB.loadDataInTimeRange(prevProfileStartTs, prevProfileStartTs + profileInterval);
            sim.run(prevProfileStartTs + profileInterval);
            prevProfileStartTs += profileInterval;
        }
    }
}
