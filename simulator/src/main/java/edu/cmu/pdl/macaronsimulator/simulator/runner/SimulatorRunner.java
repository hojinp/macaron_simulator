package edu.cmu.pdl.macaronsimulator.simulator.runner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.cmu.pdl.macaronsimulator.common.CustomLoggerGetter;
import edu.cmu.pdl.macaronsimulator.common.PrintMacaron;
import edu.cmu.pdl.macaronsimulator.simulator.application.Application;
import edu.cmu.pdl.macaronsimulator.simulator.application.ApplicationLogWriter;
import edu.cmu.pdl.macaronsimulator.simulator.commons.OSPriceInfoGetter;
import edu.cmu.pdl.macaronsimulator.simulator.commons.WorkloadConfig;
import edu.cmu.pdl.macaronsimulator.simulator.commons.WorkloadConfigGetter;
import edu.cmu.pdl.macaronsimulator.simulator.datalake.Datalake;
import edu.cmu.pdl.macaronsimulator.simulator.latency.LatencyGenerator;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.CacheEngine;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.Controller;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.DRAMServer;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.MacConf;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.OSCMServer;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ReconfigEvent;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileEvent;
import edu.cmu.pdl.macaronsimulator.simulator.profile.ProfileInfoStore;
import edu.cmu.pdl.macaronsimulator.simulator.timeline.Timeline;
import edu.cmu.pdl.macaronsimulator.simulator.timeline.TimelineManager;
import edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB;

/**
 * SimulatorRunner code. This is the initial (main) point of simulation.
 */
public class SimulatorRunner {
    public static void main(String[] args) {

        Logger logger = CustomLoggerGetter.getCustomLogger();
        logger.info("SimulatorRunner:Start running SimulatorRunner");
        PrintMacaron.printMacaron();

        Options options = new Options();
        options.addRequiredOption("d", "experimentalDirectory", true, "Experimental setup directory");
        options.addRequiredOption("p", "tracedbfilePath", true, "Path to the trace DB file");
        options.addOption("i", "isDRAMDisabled", false, "If this option is set, DRAM is disabled");
        options.addOption("o", "isOSCDisabled", false, "If this option is set, OSC is disabled");
        options.addOption("u", "interRegion", true, "Regions to be used in the simulation: use1-euc1, use1-usw1");
        options.addOption("x", "isLocalDatalake", false, "If this option is set, local data lake is enabled");
        options.addOption("r", "isDRAMOptEnabled", false, "If this option is set, DRAM optimization is enabled");
        options.addOption("t", "targetLatency", true, "Target latency for DRAM optimization");
        options.addOption("f", "isPrefetchEnabled", false, "If this option is set, prefetching is enabled");
        options.addOption("c", "miniSimDirectory", true, "Directory path to the miniature simulation result");
        options.addOption("k", "isOSCOptEnabled", false, "If this option is set, OSC size optimization is enabled");
        options.addOption("m", "isOSCOptOnce", false, "If this option is set, OSC size optimization runs only once");
        options.addOption("b", "objectStoragePriceFile", true, "Object storage price information file");
        options.addOption("h", "latencyMiniCacheCount", true, "Latency mini-cache count");
        options.addOption("a", "oscOptTimeWindowMin", true, "OSC optimization time window (minute)");
        options.addOption("e", "oscOptWarmupTimeMin", true, "OSC optimization warmup time window count");
        options.addOption("j", "enableLatencyLogging", false, "If this option is set, latency logging is enabled");
        options.addOption("n", "ProfileIntervalMinute", true, "Profile interval (minute) for ProfileEvent");
        options.addOption("g", "statDecayRate", true, "Decay rate of the statistics for adaptivity");
        options.addOption("s", "suspendUntilOptimizationTrigger", false, "If this option is set, the Macaron cache is suspended until the optimization trigger");
        options.addOption("q", "evictFirstBeforeOSCUpdate", false, "If this option is set, evict first before OSC update");
        options.addOption("l", "isDRAMOptMain", false, "If this option is set, DRAM optimization is the main optimization");
        options.addOption("z", "isDRAMOptMainOnce", false, "If this option is set, DRAM optimization runs only once");
        options.addOption("y", "preparedSolutionFile", true, "Prepared solution file for DRAM optimization");
        options.addOption("v", "logAllLatencies", false, "Log All Latencies");


        /* Start parsing give options */
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        /* Option: ExperimentalSetupDirectory */
        String expDirname = cmd.getOptionValue("d");
        String workloadConfigFilename = Paths.get(expDirname, "workloadConfig.yml").toString();
        WorkloadConfig workloadConfig = WorkloadConfigGetter.getWorkloadConfig(workloadConfigFilename);
        boolean isMacaronEnabled = workloadConfig.getMacaronCacheEnabled();
        String traceFilePath = workloadConfig.getTraceFilename();
        workloadConfig.printConfigs();
        if (!Files.exists(Paths.get(traceFilePath)) || !Files.exists(Paths.get(expDirname)))
            throw new RuntimeException("Trace file or experimental directory does not exist: " + expDirname);

        String profileLogDirname = Paths.get(expDirname, "output").toString();
        if (Files.exists(Paths.get(profileLogDirname)))
            throw new RuntimeException("Output directory already exists: " + profileLogDirname);

        try {
            Files.createDirectories(Paths.get(profileLogDirname));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ProfileInfoStore.LOG_DIRNAME = profileLogDirname;
        if (cmd.hasOption("j")) {
            ApplicationLogWriter.LOG_FILENAME = Paths.get(profileLogDirname, "app_latency.csv").toString();
            if (ApplicationLogWriter.DETAILED)
                ApplicationLogWriter.DETAILED_LOG_FILENAME = Paths.get(profileLogDirname, "app_latency_detailed.csv").toString();
        }

        /* Option: ProfileIntervalMinute */
        if (cmd.hasOption("n"))
            ProfileEvent.profileInterval = Long.parseLong(cmd.getOptionValue("n")) * 60L * 1000L * 1000L;

        /* Option: DRAMDisable */
        boolean isDramEnabled = !cmd.hasOption("i");
        CacheEngine.IS_DRAM_ENABLED = isDramEnabled;
        logger.info("* DRAM is " + (isDramEnabled ? "enabled" : "disabled"));

        /* Option: OSCDisable */
        boolean isOSCEnabled = !cmd.hasOption("o");
        CacheEngine.IS_OSC_ENABLED = isOSCEnabled;
        logger.info("* OSC is " + (isOSCEnabled ? "enabled" : "disabled"));

        /* Option: InterRegion */
        if (cmd.hasOption("u"))
            LatencyGenerator.setInterRegions(cmd.getOptionValue("u"));
        LatencyGenerator.IS_DRAM_ENABLED = isDramEnabled;

        /* Option: LocalDatalake */
        LatencyGenerator.IS_LOCAL_DATALAKE = cmd.hasOption("x");
        if (LatencyGenerator.IS_LOCAL_DATALAKE && isMacaronEnabled)
            throw new RuntimeException("Macaron cannot be enabled when local data lake is enabled");
        LatencyGenerator.setup(isMacaronEnabled);

        /* Option: MiniatureSimulationResultDirectory */
        Controller.miniSimDirname = cmd.getOptionValue("c", null);

        /* Option: DRAM reconfiguration plan */
        ReconfigEvent.IS_DRAM_OPT_ENABLED = cmd.hasOption("r");
        if (ReconfigEvent.IS_DRAM_OPT_ENABLED) {
            if (!isDramEnabled || !cmd.hasOption("c") || !cmd.hasOption("t") || !cmd.hasOption("h"))
                throw new RuntimeException("DRAM, \"c\", \"t\", \"h\" options must be enabled");
            ReconfigEvent.TARGET_LATENCY = Long.parseLong(cmd.getOptionValue("t"));
            Controller.LATENCY_MINI_CACHE_COUNT = Integer.parseInt(cmd.getOptionValue("h"));
            Logger.getGlobal().info("* DRAM reconfig target latency: " + String.valueOf(ReconfigEvent.TARGET_LATENCY));
        }


        /* Option: DRAM Prefetching to DRAM */
        ReconfigEvent.IS_PREFETCH_ENABLED = cmd.hasOption("f");
        if (ReconfigEvent.IS_PREFETCH_ENABLED && (!ReconfigEvent.IS_DRAM_OPT_ENABLED || !isOSCEnabled))
            throw new RuntimeException("DRAM optimization and OSC must be enabled to use prefetching");

        /* Option: EnableOSCSizeOptimize */
        ReconfigEvent.IS_OSC_OPT_ENABLED = cmd.hasOption("k");
        if (isMacaronEnabled) {
            if (!cmd.hasOption("a") || !cmd.hasOption("e"))
                throw new RuntimeException("OSC optimization time window and warmup time window must be set (even if optimization is disabled)");

            ReconfigEvent.setStatWindow(Integer.parseInt(cmd.getOptionValue("a")),
                    Integer.parseInt(cmd.getOptionValue("e")), cmd.hasOption("g"));
        }
        if (ReconfigEvent.IS_OSC_OPT_ENABLED) {
            if (cmd.hasOption("m"))
                ReconfigEvent.IS_OSC_OPT_ONCE = true;

            if (cmd.hasOption("g"))
                ReconfigEvent.setStatTargetWeightAfterDay(Double.parseDouble(cmd.getOptionValue("g")));

            if (!isOSCEnabled || !cmd.hasOption("c"))
                throw new RuntimeException("OSC and \"c\" options must be enabled to use OSC size optimization");

            Logger.getGlobal().info("* OSC size optimization is enabled");
            Logger.getGlobal().info("* OSC optimization time window: " + ReconfigEvent.getStatWindowMin() + " min");
            Logger.getGlobal().info("* OSC optimization warmup time: " + ReconfigEvent.getStatWarmupMin() + " min");
        }

        /* Option: DRAM_OPT_IS_MAIN or ONCE */
        ReconfigEvent.DRAM_OPT_IS_MAIN = cmd.hasOption("l");
        ReconfigEvent.DRAM_OPT_MAIN_ONCE = cmd.hasOption("z");
        if (ReconfigEvent.DRAM_OPT_IS_MAIN) {
            if (CacheEngine.IS_OSC_ENABLED || ReconfigEvent.IS_OSC_OPT_ENABLED || ReconfigEvent.IS_DRAM_OPT_ENABLED 
                || !CacheEngine.IS_DRAM_ENABLED || ReconfigEvent.IS_PREFETCH_ENABLED)
                throw new RuntimeException("DRAM optimization must be the main optimization");
        }

        /* Option: SuspendUntilOptimizationTrigger */
        if (cmd.hasOption("s"))
            CacheEngine.IS_SUSPENDED = true;

        /* Option: OSCostInfoFilepath */
        if (cmd.hasOption("b"))
            OSPriceInfoGetter.customOSCostInfoFilepath = cmd.getOptionValue("b");

        /* Option: PreparedSolution */
        if (cmd.hasOption("y"))
            Controller.preparedSolution = cmd.getOptionValue("y");

        if (cmd.hasOption("v"))
            ApplicationLogWriter.LOG_ALL = true;
        /* End of the option parsing */

        /* Prepare traceDataDB */
        TraceDataDB.traceFilePath = traceFilePath;
        TraceDataDB.dbFilePath = cmd.getOptionValue("p");
        TraceDataDB.initialize();

        /* Prepare application and datalake */
        Application application = new Application();
        Datalake datalake = new Datalake();
        datalake.prepareDataForTrace(traceFilePath);

        /* If Macaron cache exists, configure all the components. */
        Controller controller = null;
        if (isMacaronEnabled) {
            final String macaronCacheConfigFilename = Paths.get(expDirname, "macaronCacheConfig.yml").toString();
            MacConf.setConfiguration(macaronCacheConfigFilename);
            controller = new Controller();
        }

        /* Connect application to CacheEngine or Datalake. */
        application.connect(isMacaronEnabled);

        /* Create timeline and timelineManager that manages event registers/triggers. */
        final Timeline timeline = new Timeline();
        final TimelineManager timelineManager = new TimelineManager(timeline);
        TimelineManager.registerComponent(MacConf.APP_NAME, application);
        TimelineManager.registerComponent(MacConf.DATALAKE_NAME, datalake);
        if (isMacaronEnabled) {
            if (controller == null)
                throw new RuntimeException("MacaronMaster == null");
            if (CacheEngine.IS_OSC_ENABLED) {
                TimelineManager.registerComponent(MacConf.OSC_NAME, controller.getOSC());
                TimelineManager.registerComponent(MacConf.OSCM_NAME, controller.getOSCMServer());
            }
            for (final CacheEngine cacheEngine : controller.getCacheEngines())
                TimelineManager.registerComponent(cacheEngine.NAME, cacheEngine);
            for (final DRAMServer dramServer : controller.getDRAMServers())
                TimelineManager.registerComponent(dramServer.getName(), dramServer);
            TimelineManager.registerComponent(MacConf.CONTROLLER_NAME, controller);
        }

        /* Start application's request queue and start running timelineManager */
        Future<Boolean> requestQueueSuccess = application.startRequestQueue();
        timelineManager.registerApplicationRequest(application);
        logger.info("[Start] timelineManager.runTimeline()");
        timelineManager.runTimeline(Datalake.totalRequestCount);
        logger.info("[Finished] timelineManager.runTimeline()");
        application.stop();

        try {
            if (!requestQueueSuccess.get())
                throw new RuntimeException("This Application's RequestQueue is failed.");
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
