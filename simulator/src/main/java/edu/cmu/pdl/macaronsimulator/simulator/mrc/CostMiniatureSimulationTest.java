package edu.cmu.pdl.macaronsimulator.simulator.mrc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import edu.cmu.pdl.macaronsimulator.common.CustomLoggerGetter;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ReconfigEvent;

public class CostMiniatureSimulationTest {
    static double egressPrice = 0.09;

    static double computeExpectedCost(int cacheSizeMB, double mr, long mb, int getCnt, int putCnt,
            double avgBlockPortion) {
        double capacityCost = (cacheSizeMB / 1024.)
                * (0.023 / 30.5 / 24. / 60. * ReconfigEvent.getStatWindowMin()) / avgBlockPortion;
        double dataTransferCost = (mb / 1024. / 1024. / 1024.) * egressPrice;
        double putOpCost = (getCnt * mr + putCnt) * 0.005 / 1000.;
        return capacityCost + dataTransferCost + putOpCost;
    }

    public static void main(String[] args) {
        Logger logger = CustomLoggerGetter.getCustomLogger();

        Options options = new Options();
        options.addRequiredOption("m", "minisimDirname", true, "Path to the miniature simulation directory");
        options.addRequiredOption("o", "outputdir", true, "Output directory name");
        options.addRequiredOption("c", "cross", true, "cross_cloud or cross_region");
        options.addOption("e", "enableDecay", false, "Enable decay factor (default: false)");
        options.addOption("d", "decayTarget", true, "Decay target weight after a week");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String minisimDirname = cmd.getOptionValue("m");
        String outDirname = cmd.getOptionValue("o");
        String cross = cmd.getOptionValue("c");
        if (!cross.equals("cross_cloud") && !cross.equals("cross_region"))
            throw new RuntimeException("Invalid cross value: " + cross);
        CostMiniatureSimulationTest.egressPrice = cross.equals("cross_cloud") ? 0.09 : 0.02;
        
        double decayTarget = 1.;
        if (cmd.hasOption("e")) {
            decayTarget = Double.parseDouble(cmd.getOptionValue("d"));
        }

        String outFilename;
        if (cmd.hasOption("e")) {
            outFilename = outDirname + "/optimization_" + String.format("%.4f", decayTarget) + ".csv";
        } else {
            outFilename = outDirname + "/optimization_nodecay.csv";
        }
        
        // Check if outFilename already exists
        File outFile = new File(outFilename);
        if (outFile.exists()) {
            logger.info("Output file already exists: " + outFilename);
            return;
        }

        // create outDirname
        File outDir = new File(outDirname);
        if (!outDir.isDirectory()) {
            outDir.mkdirs();
        }

        logger.info("Miniature simulation directory: " + minisimDirname);
        logger.info("Output directory: " + outDirname);
        if (cmd.hasOption("e")) {
            logger.info("Decay is enabled");
            logger.info("Decay factor: " + decayTarget);
        } else {
            logger.info("Decay is disabled");
        }

        ReconfigEvent.setStatWindow(15, 1440, cmd.hasOption("e"));
        if (cmd.hasOption("e")) {
            ReconfigEvent.setStatTargetWeightAfterDay(decayTarget);
        }
        CostMiniatureSimulation minisim = new CostMiniatureSimulation(minisimDirname);

        List<Integer> minuteList = minisim.getMinuteList();
        int lastMinute = minuteList.get(minuteList.size() - 1);

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(outFilename));
            writer.write("minute,optimalCacheSizeMB\n");

            double samplingRatio = 0.05;
            int optInterval = 15;
            for (int minute = 60 * 24 + optInterval; minute <= lastMinute; minute += optInterval) {
                int[] cacheSizeMBs = minisim.getCacheSizeMBs();
                double[] mrc = minisim.getMRC(minute);
                long[] bmc = minisim.getBMC(minute);
                int sampledRequestCount = minisim.getSampledRequestCount(minute);
                assert (sampledRequestCount != -1);
                int requestCount = (int) (sampledRequestCount / samplingRatio); // XXX: approximate value for fast testing
                double avgBlockPortion = 0.7; // XXX: approximate value for fast testing

                int optimalCacheSizeMB = -1;
                double minCost = Double.MAX_VALUE;
                for (int i = 0; i < cacheSizeMBs.length; i++) {
                    double cost = computeExpectedCost(cacheSizeMBs[i], mrc[i], bmc[i], requestCount, 0, avgBlockPortion);
                    optimalCacheSizeMB = cost < minCost ? cacheSizeMBs[i] : optimalCacheSizeMB;
                    minCost = cost < minCost ? cost : minCost;
                }
                writer.write(minute + "," + optimalCacheSizeMB + "\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
