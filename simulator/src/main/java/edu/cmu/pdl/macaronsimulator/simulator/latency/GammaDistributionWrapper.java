package edu.cmu.pdl.macaronsimulator.simulator.latency;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.regression.SimpleRegression;

public class GammaDistributionWrapper {
    private static final double MIN_BUFF = 1.0;
    private final double minValue;
    private final double maxValue;
    private final GammaDistribution dist;

    private final int candidateCnt = 100000;
    private final int bufferCnt = 10;
    private double randValueCandidates[][];
    private double medValueCandidates[][];
    private boolean randBufferReady[];
    private boolean medBufferReady[];
    private int randBufferIdx = 0;
    private int randValueIdx = 0;
    private int medBufferIdx = 0;
    private int medValueIdx = 0;

    private final ExecutorService executor;
    private final static int THREAD_CNT = 12;

    public GammaDistributionWrapper(final double[] data) {
        assert data.length > 0;
        Arrays.sort(data);
        assert data[0] > MIN_BUFF;
        this.minValue = data[0] - MIN_BUFF;
        int maxIdx = (int) (data.length * 1.) - 1; /* consider Top 1% as an outlier */
        this.maxValue = data[maxIdx];
        for (int idx = 0; idx < data.length; idx++)
            data[idx] -= minValue;

        dist = fitGammaDistribution(data);
        dist.reseedRandomGenerator(0); /* default seed of the generator is 0 */
        randValueCandidates = new double[bufferCnt][candidateCnt];
        medValueCandidates = new double[bufferCnt][candidateCnt];
        randBufferReady = new boolean[bufferCnt];
        medBufferReady = new boolean[bufferCnt];
        for (int idx = 0; idx < bufferCnt; idx++) {
            genRandValueCandidates(idx);
            randBufferReady[idx] = true;
            genMedValueCandidates(idx);
            medBufferReady[idx] = true;
        }
        executor = Executors.newFixedThreadPool(THREAD_CNT);
    }


    private static GammaDistribution fitGammaDistribution(double[] data) {
        SummaryStatistics stats = new SummaryStatistics();
        for (double value : data) {
            stats.addValue(value);
        }

        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < data.length; i++) {
            regression.addData(Math.log(data[i]), 1);
        }

        double mean = stats.getMean();
        double variance = stats.getVariance();

        double shape = mean * mean / variance;
        double scale = variance / mean;

        return new GammaDistribution(shape, scale);
    }


    private void genRandValueCandidates(int idx) {
        for (int i = 0; i < candidateCnt; i++) {
            double newRandValue = minValue + dist.sample();
            if (newRandValue < minValue || newRandValue > maxValue) {
                i--;
                continue;
            }
            randValueCandidates[idx][i] = newRandValue;
        }
    }

    private void asyncRandBufferGen(int idx) {
        executor.submit(() -> {
            genRandValueCandidates(idx);
            randBufferReady[idx] = true;
        });
    }

    private void genMedValueCandidates(int idx) {
        for (int i = 0; i < candidateCnt; i++) {
            double newMedValue = minValue + dist.getNumericalMean();
            if (newMedValue < minValue || newMedValue > maxValue) {
                i--;
                continue;
            }
            medValueCandidates[idx][i] = newMedValue;
        }
    }

    private void asyncMedBufferGen(int idx) {
        executor.submit(() -> {
            genMedValueCandidates(idx);
            medBufferReady[idx] = true;
        });
    }

    public double getValue() {
        final double ret;
        if (randValueIdx == candidateCnt) {
            randBufferIdx = (randBufferIdx + 1) % bufferCnt;
            randValueIdx = 0;
            assert randBufferReady[randBufferIdx];
            ret = randValueCandidates[randBufferIdx][randValueIdx];
            randValueIdx += 1;
        } else {
            ret = randValueCandidates[randBufferIdx][randValueIdx];
            randValueIdx += 1;
        }
        return ret;
    }

    public double getMedValue() {
        final double ret;
        if (medValueIdx == candidateCnt) {
            medBufferIdx = (medBufferIdx + 1) % bufferCnt;
            medValueIdx = 0;
            assert medBufferReady[medBufferIdx];
            ret = medValueCandidates[medBufferIdx][medValueIdx];
            medValueIdx += 1;
        } else {
            ret = medValueCandidates[medBufferIdx][medValueIdx];
            medValueIdx += 1;
        }
        return ret;
    }

    public void waitDone() {
        try {
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
