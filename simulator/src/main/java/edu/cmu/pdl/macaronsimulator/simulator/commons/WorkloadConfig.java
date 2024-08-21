package edu.cmu.pdl.macaronsimulator.simulator.commons;

import java.util.logging.Logger;

/**
 * Stores the workload configurations.
 */
public class WorkloadConfig {

    private String traceFilename;
    private boolean macaronCacheEnabled;

    public WorkloadConfig() {
    }

    public String getTraceFilename() {
        return traceFilename;
    }

    public void setTraceFilename(String traceFilename) {
        this.traceFilename = traceFilename;
    }

    public boolean getMacaronCacheEnabled() {
        return macaronCacheEnabled;
    }

    public void setMacaronCacheEnabled(boolean macaronCacheEnabled) {
        this.macaronCacheEnabled = macaronCacheEnabled;
    }

    public void printConfigs() {
        Logger.getGlobal().info("Workload configurations: ");
        Logger.getGlobal().info("* Trace filename: " + traceFilename);
        Logger.getGlobal().info("* Macaron cache enabled: " + macaronCacheEnabled);
        Logger.getGlobal().getHandlers()[0].flush();
    }
}
