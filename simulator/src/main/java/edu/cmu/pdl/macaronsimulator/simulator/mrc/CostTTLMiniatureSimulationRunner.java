package edu.cmu.pdl.macaronsimulator.simulator.mrc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.cmu.pdl.macaronsimulator.common.CustomLoggerGetter;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.cache.CacheType;
import edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB;

public class CostTTLMiniatureSimulationRunner {
    public static void main(String[] args) {
        Logger logger = CustomLoggerGetter.getCustomLogger();
        logger.info("[MiniatureSimluationRunner] Start running");

        // Options
        Options options = new Options();
        options.addRequiredOption("f", "TraceFilename", true, "Path to the trace filename");
        options.addRequiredOption("o", "OutputDirname", true, "Output directory name");
        options.addRequiredOption("s", "SamplingRatio", true, "Sampling ratio of MiniatureSimulation");
        options.addRequiredOption("c", "MiniCacheCount", true, "The number of miniature caches");
        options.addRequiredOption("k", "MaxObjectId", true, "Maximum object id of the given trace");
        options.addRequiredOption("p", "tracedbfilePath", true, "Path to the trace DB file");
        options.addRequiredOption("m", "ProfileIntervalMinutes", true, "Profile interval in minutes (default: 15)");

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
        CacheType cacheType = CacheType.TTL;
        int maxObjectId = Integer.parseInt(cmd.getOptionValue("k"));

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

        CostTTLMiniatureSimulation sim = new CostTTLMiniatureSimulation(outDirname, samplingRatio, miniCacheCount, cacheType, maxObjectId);

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
