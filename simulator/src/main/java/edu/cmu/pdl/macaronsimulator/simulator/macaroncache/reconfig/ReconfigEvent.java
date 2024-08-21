package edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig;

import edu.cmu.pdl.macaronsimulator.simulator.event.Event;
import edu.cmu.pdl.macaronsimulator.simulator.event.EventType;

public class ReconfigEvent implements Event {
    public static boolean IS_OSC_OPT_ENABLED = false;
    public static boolean IS_OSC_OPT_ONCE = false;
    public static boolean IS_DRAM_OPT_ENABLED = false;
    public static long TARGET_LATENCY = 0L;
    public static boolean IS_PREFETCH_ENABLED = false;
    public static boolean UPDATE_OSC_FIRST_EVICT_LATER = true;
    private final boolean init;

    public static boolean DRAM_OPT_IS_MAIN = false;
    public static boolean DRAM_OPT_MAIN_ONCE = false;

    public static long oscReconfigInterval = 0L;
    public static long oscReconfigStTime = 0L;
    public static long oscGCInterval = 15L * 60L * 1000L * 1000L; // XXX: GC every 15 minutes /* ROLLBACK */
    public static long dramReconfigInterval = 15L * 60L * 1000L * 1000L; // XXX: reconfigure every 15 minutes
    // public static long oscGCInterval = 10L * 60L * 1000L * 1000L; // XXX: GC every 15 minutes
    // public static long dramReconfigInterval = 10L * 60L * 1000L * 1000L; // XXX: reconfigure every 15 minutes
    public static long dramReconfigStTime = 0L; // Starts IMC reconfiguration after 4 days

    // Exponential decay of MRC, BMC, and other statistics over time
    private static boolean isStatWindowReady = false;
    private static boolean isStatDecayEnabled = false;
    private static int statWindowMin = -1, statWarmupTimeMin = -1;
    private static double statTargetWeightAfterDay = 0.2;
    private static double statDecayRate = -1.;

    public ReconfigEvent(final boolean init) {
        this.init = init;
    }

    @Override
    public EventType getEventType() {
        return EventType.RECONFIGURE;
    }

    /**
     * Return whether the event is for initialization
     * 
     * @return
     */
    public boolean isInit() {
        return init;
    }

    /**
     * Set the time window (in hour) for the exponential decay of MRC, BMC over time. Update the decay rate accordingly.
     * 
     * @param statWindowHr the time window (in hour) for the exponential decay of MRC, BMC over time
     * @param statWarmupWindowCnt the number of time windows to warm up the simulation
     */
    public static void setStatWindow(int statWindowMin, int statWarmupTimeMin, boolean isStatDecayEnabled) {
        if (isStatWindowReady)
            throw new RuntimeException("Stat window is already set");
        isStatWindowReady = true;
        ReconfigEvent.isStatDecayEnabled = isStatDecayEnabled;
        ReconfigEvent.statWindowMin = statWindowMin;
        ReconfigEvent.statWarmupTimeMin = statWarmupTimeMin;
        ReconfigEvent.statDecayRate = -Math.log(statTargetWeightAfterDay) / (24. * 60. / statWindowMin);
        ReconfigEvent.oscReconfigInterval = statWindowMin * 60L * 1000L * 1000L; /* ROLLBACK */
        ReconfigEvent.oscReconfigStTime = statWarmupTimeMin * 60L * 1000L * 1000L;
        ReconfigEvent.dramReconfigStTime = statWarmupTimeMin * 60L * 1000L * 1000L;
        // ReconfigEvent.oscReconfigInterval = 10L * 60L * 1000L * 1000L;
        // ReconfigEvent.oscReconfigStTime = 20L * 60L * 1000L * 1000L;
        // ReconfigEvent.dramReconfigStTime = 20L * 60L * 1000L * 1000L;
    }

    public static void setStatTargetWeightAfterDay(double statTargetWeightAfterDay) {
        if (!isStatWindowReady || !isStatDecayEnabled)
            throw new RuntimeException("Stat window is not set or decay is not enabled yet");
        ReconfigEvent.statTargetWeightAfterDay = statTargetWeightAfterDay;
        ReconfigEvent.statDecayRate = -Math.log(statTargetWeightAfterDay) / (24. * 60. / ReconfigEvent.statWindowMin);
    }

    /**
     * Set the GC interval (in minute) for OSC
     * 
     * @param oscGCIntervalMinute the GC interval (in minute) for OSC
     */
    public static void setOSCGCIntervalMinute(int oscGCIntervalMinute) {
        throw new RuntimeException("DO NOT USE THIS OPTION");
        // ReconfigEvent.oscGCInterval = oscGCIntervalMinute * 60L * 1000L * 1000L;
    }

    public static long getOSCGCInterval() {
        if (ReconfigEvent.oscGCInterval == 0L)
            throw new RuntimeException("OSC GC interval is not set yet");
        return ReconfigEvent.oscGCInterval;
    }

    /**
     * Return the time window (in minutes)
     * 
     * @return the time window (in minutes)
     */
    public static int getStatWindowMin() {
        if (!isStatWindowReady)
            throw new RuntimeException("Stat window is not set yet");
        return statWindowMin;
    }

    /**
     * Return the warmup time hrs
     * 
     * @return the warmup time hrs
     */
    public static int getStatWarmupMin() {
        if (!isStatWindowReady)
            throw new RuntimeException("Stat window is not set yet");
        return statWarmupTimeMin;
    }

    /**
     * Return the coefficients that will be multiplied to the local MRC and BMC values (considering weight and dacay)
     * 
     * @param localCnts the number of requests in each current time window (localCnts[0]: current time window, localCnts[1]: previous time window, ...)
     * @return the coefficient thats will be multiplied to the local MRC and BMC values
     */
    public static double[] getWeightDecayCoefficient(int[] localCnts) {
        if (localCnts.length == 0)
            return null;

        double[] coefficients = new double[localCnts.length];
        double[] weightCoefficients = new double[localCnts.length];
        double[] decayCoefficients = isStatDecayEnabled ? new double[localCnts.length] : null;

        int sumCnts = 0;
        for (int cnt : localCnts)
            sumCnts += cnt;
        if (sumCnts == 0)
            return null;

        for (int i = 0; i < localCnts.length; i++) {
            weightCoefficients[i] = (double) localCnts[i] / sumCnts;
        }

        if (isStatDecayEnabled) {
            for (int i = 0; i < localCnts.length; i++)
                decayCoefficients[i] = Math.exp(-statDecayRate * i);
            double sumDecayCoefficients = 0.;
            for (double decayCoefficient : decayCoefficients)
                sumDecayCoefficients += decayCoefficient;
            for (int i = 0; i < localCnts.length; i++)
                decayCoefficients[i] /= sumDecayCoefficients;
        }

        for (int i = 0; i < localCnts.length; i++)
            coefficients[i] = weightCoefficients[i] * (isStatDecayEnabled ? decayCoefficients[i] : 1.);
        double sumCoefficients = 0.;
        for (double coefficient : coefficients)
            sumCoefficients += coefficient;
        for (int i = 0; i < localCnts.length; i++)
            coefficients[i] /= sumCoefficients;

        return coefficients;
    }

    /**
     * Return the coefficients that will be multiplied to the local MRC and BMC values (considering dacay)
     * 
     * @param localCnts the number of requests in each current time window (localCnts[0]: current time window, localCnts[1]: previous time window, ...)
     * @return the coefficient thats will be multiplied to the local MRC and BMC values
     */
    public static double[] getDecayCoefficient(int length) {
        if (length == 0)
            return null;

        double[] decayCoefficients = new double[length];

        if (isStatDecayEnabled) {
            for (int i = 0; i < length; i++)
                decayCoefficients[i] = Math.exp(-statDecayRate * i);

            double sumDecayCoefficients = 0.;
            for (double decayCoefficient : decayCoefficients)
                sumDecayCoefficients += decayCoefficient;
            for (int i = 0; i < length; i++)
                decayCoefficients[i] /= sumDecayCoefficients;
        } else {
            for (int i = 0; i < length; i++)
                decayCoefficients[i] = 1. / length;
        }

        return decayCoefficients;
    }
}
