package edu.cmu.pdl.macaronsimulator.simulator.application;

import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang3.tuple.Triple;

/**
 * Writes the application log to a file. The log is written in CSV format.
 * 
 * The log file contains the following information:
 * 1. Timestamp: The time when the request is ended.
 * 2. Object size: The size of the object in bytes.
 * 3. Latency: The time between the object creation and the object being written to the file.
 */
public class ApplicationLogWriter {

    public static String LOG_FILENAME = null;
    public static String DETAILED_LOG_FILENAME = null;
    public static boolean DETAILED = true;
    public static boolean LOG_ALL = false;

    private final List<Long> latencies = new ArrayList<>();
    private final List<Integer> srcs = new ArrayList<>();

    private final List<Long> bufferLatencies = new ArrayList<>();
    private final List<Long> dramLatencies = new ArrayList<>();
    private final List<Long> oscLatencies = new ArrayList<>();
    private final List<Long> datalakeLatencies = new ArrayList<>();
    
    private final static long MIN_US = 60L * 1000L * 1000L;

    public ApplicationLogWriter() {
    }

    /**
     * Adds a log entry to the log queue.
     * @param log The log entry to be added.
     */
    public void log(final Triple<Long, CompType, Long> log) {
        if (LOG_FILENAME != null) {
            latencies.add(log.getRight());
            if (true) {
                assert log.getMiddle() == CompType.ENGINE || log.getMiddle() == CompType.DRAM || log.getMiddle() == CompType.OSC || log.getMiddle() == CompType.DATALAKE;
                srcs.add(log.getMiddle() == CompType.ENGINE || log.getMiddle() == CompType.DRAM ? 0 : log.getMiddle() == CompType.OSC ? 1 : 2);
            }
            if (DETAILED) {
                switch (log.getMiddle()) {
                    case ENGINE:
                        bufferLatencies.add(log.getRight());
                        break;
                    case DRAM:
                        dramLatencies.add(log.getRight());
                        break;
                    case OSC:
                        oscLatencies.add(log.getRight());
                        break;
                    case DATALAKE:
                        datalakeLatencies.add(log.getRight());
                        break;
                    default:
                        break;
                }
            }
        }
    }

    /**
     * Saves the latency information to the file.
     * 
     * @param ts The timestamp when it saves the latency information.
     */
    public void saveLatencyInfoToFile(long ts) {
        if (LOG_FILENAME != null) {
            try {
                // if file does not exist, write a header
                File file = new File(LOG_FILENAME);
                if (!file.exists()) {
                    BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILENAME, false));
                    writer.write("Minute,Count,AvgLatency\n");
                    writer.close();
                }

                double avgLatency = 0.;
                if (latencies.size() > 0) {
                    for (long latency : latencies) {
                        avgLatency += (double) latency / (double) latencies.size();
                    }
                } else {
                    avgLatency = 0.;
                }
                int curMin = (int) (ts / MIN_US);
                BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILENAME, true));
                writer.write(String.format("%d,%d,%s\n", curMin, latencies.size(), String.valueOf((long) avgLatency)));
                writer.close();

                if (LOG_ALL) {
                    // parent directory of LOG_FILENAME and filename is app_latency_all.csv
                    String newfilename = LOG_FILENAME.substring(0, LOG_FILENAME.lastIndexOf("/")) + "/app_latency_all.csv";
                    File newfile = new File(newfilename);
                    if (!newfile.exists()) {
                        writer = new BufferedWriter(new FileWriter(newfilename, false));
                        writer.write("Minute,Latency,Src\n");
                        writer.close();
                    }
                    writer = new BufferedWriter(new FileWriter(newfilename, true));
                    assert latencies.size() == srcs.size();
                    for (int idx = 0; idx < latencies.size(); idx++) {
                        writer.write(String.format("%d,%d,%d\n", curMin, latencies.get(idx), srcs.get(idx)));
                    }
                    writer.close();
                }

                latencies.clear();
                srcs.clear();

                if (DETAILED) {
                    file = new File(DETAILED_LOG_FILENAME);
                    if (!file.exists()) {
                        writer = new BufferedWriter(new FileWriter(DETAILED_LOG_FILENAME, false));
                        writer.write("Minute,BufferCount,BufferLatency,DRAMCount,DRAMLatency,OSCCount,OSCLatency,DLCount,DLLatency\n");
                        writer.close();
                    }

                    double avgBufferLatency = 0.;
                    if (bufferLatencies.size() > 0) {
                        for (long latency : bufferLatencies) {
                            avgBufferLatency += (double) latency / (double) bufferLatencies.size();
                        }
                    } else {
                        avgBufferLatency = 0.;
                    }

                    double avgDRAMLatency = 0.;
                    if (dramLatencies.size() > 0) {
                        for (long latency : dramLatencies) {
                            avgDRAMLatency += (double) latency / (double) dramLatencies.size();
                        }
                    } else {
                        avgDRAMLatency = 0.;
                    }

                    double avgOSCLatency = 0.;
                    if (oscLatencies.size() > 0) {
                        for (long latency : oscLatencies) {
                            avgOSCLatency += (double) latency / (double) oscLatencies.size();
                        }
                    } else {
                        avgOSCLatency = 0.;
                    }

                    double avgDLLatency = 0.;
                    if (datalakeLatencies.size() > 0) {
                        for (long latency : datalakeLatencies) {
                            avgDLLatency += (double) latency / (double) datalakeLatencies.size();
                        }
                    } else {
                        avgDLLatency = 0.;
                    }

                    writer = new BufferedWriter(new FileWriter(DETAILED_LOG_FILENAME, true));
                    writer.write(String.format("%d,%d,%s,%d,%s,%d,%s,%d,%s\n", curMin, 
                        bufferLatencies.size(), String.valueOf((long) avgBufferLatency), 
                        dramLatencies.size(), String.valueOf((long) avgDRAMLatency), 
                        oscLatencies.size(), String.valueOf((long) avgOSCLatency), 
                        datalakeLatencies.size(), String.valueOf((long) avgDLLatency)));
                    writer.close();

                    bufferLatencies.clear();
                    dramLatencies.clear();
                    oscLatencies.clear();
                    datalakeLatencies.clear();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
