package edu.cmu.pdl.macaronsimulator.simulator.latency;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.math3.stat.regression.SimpleRegression;

public class RequestLatency {
    private boolean setup = false, avgSetup = false;
    private Map<Long, long[]> latencyMap = new HashMap<>();
    private Map<Long, GammaDistributionWrapper> sizeToLatencies = new HashMap<>();
    private long[] datasizes;
    private long maxObsDatasize;
    public double regSlope;
    private double avgSlope;

    public RequestLatency() {
    }

    public void addData(long datasize, long[] latencies) {
        assert !setup;
        latencyMap.put(datasize, latencies);
    }

    public void setup() {
        assert !setup && !avgSetup;
        setup = true;

        /* generate data size to latency generator */
        datasizes = new long[latencyMap.size()];
        int dsIdx = 0;
        for (long datasize : latencyMap.keySet()) {
            double[] latencies = new double[latencyMap.get(datasize).length];
            for (int i = 0; i < latencies.length; i++)
                latencies[i] = (double) latencyMap.get(datasize)[i];
            GammaDistributionWrapper dist = new GammaDistributionWrapper(latencies);
            sizeToLatencies.put(datasize, dist);
            datasizes[dsIdx++] = datasize;
        }
        Arrays.sort(datasizes);
        maxObsDatasize = datasizes[datasizes.length - 1];
        assert datasizes[0] == 1L;

        /* for larger than the largest object size, it uses avg. slope. */
        SimpleRegression regression = new SimpleRegression();
        for (int i = datasizes.length - 3; i < datasizes.length; i++)
            regression.addData(datasizes[i], sizeToLatencies.get(datasizes[i]).getMedValue());
        regSlope = regression.getSlope();
    }

    public void waitDone() {
        for (GammaDistributionWrapper latencies : sizeToLatencies.values())
            latencies.waitDone();
    }

    public void setAvgSlope(double regSlope2) {
        assert setup && !avgSetup;
        avgSetup = true;
        avgSlope = (regSlope + regSlope2) / 2.;
    }

    public void setAvgSlope() {
        assert setup && !avgSetup;
        avgSetup = true;
        avgSlope = regSlope;
    }

    public long getLatency(long objSize) {
        assert setup && avgSetup;
        long latency = 0L;
        objSize = Math.max(1L, objSize);

        if (objSize == 1L) {
            latency = (long) sizeToLatencies.get(1L).getValue();
        } else if (objSize <= maxObsDatasize) { // can interpolate with observed data
            for (int i = 1; i < datasizes.length; i++) {
                if (datasizes[i] >= objSize) {
                    long highSize = datasizes[i];
                    double highMedLat = sizeToLatencies.get(highSize).getMedValue();
                    long lowSize = datasizes[i - 1];
                    double lowLat = sizeToLatencies.get(lowSize).getValue();
                    double lowMedLat = sizeToLatencies.get(lowSize).getMedValue();
                    latency = (long) ((highMedLat - lowMedLat) * (objSize - lowSize) / (highSize - lowSize) + lowLat);
                    break;
                }
            }
        } else { // can extrapolate using the average slope
            double baseLat = sizeToLatencies.get(maxObsDatasize).getValue();
            latency = (long) (baseLat + avgSlope * (objSize - maxObsDatasize));
        }

        assert latency > 0L;
        return latency;
    }
}
