package edu.cmu.pdl.macaronsimulator.simulator.latency;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;

public class LatencyGenerator {
    private static boolean setupComplete = false;

    public static boolean IS_DRAM_ENABLED = true;
    public static boolean IS_LOCAL_DATALAKE = false;
    public static String INTER_REGIONS = "use1-euc1"; // option: "use1-usw1"

    private static RequestLatency appProxyLatencyIMC = null;
    private static RequestLatency appProxyLatencyOSC = null;
    private static RequestLatency appProxyLatencyDL = null;
    private static RequestLatency proxyIMCLatency = null;
    private static RequestLatency proxyOSCMLatency = null;
    private static RequestLatency proxyOSCLatency = null;
    private static RequestLatency proxyDLLatency = null;
    private static RequestLatency oscmOSCLatency = null;

    private static RequestLatency appDLLatency = null;

    private static Map<Long, Map<DataType, List<Long>>> e2eLats = new HashMap<>();
    private static Map<Long, Map<DataType, List<Long>>> detailedLats = new HashMap<>();

    private static List<Long> sizes = new ArrayList<>();
    private static Map<Long, Map<DataType, Long>> e2eMedLats = new HashMap<>();
    private static Map<Long, Map<DataType, Long>> e2eAvgLats = new HashMap<>();

    private static enum DataType {
        IMC, OSCM, OSC, DL, P2PIMC, P2POSC, P2PDL, E2EIMC, E2EOSC, E2EDL
    }

    public LatencyGenerator() {
    }

    public static void setInterRegions(String interRegions) {
        if (!interRegions.equals("use1-euc1") && !interRegions.equals("use1-usw1"))
            throw new RuntimeException("Unexpected interRegions: " + interRegions);
        INTER_REGIONS = interRegions;
    }

    public static void setup(final boolean cacheEnabled) {
        assert !setupComplete;
        setupComplete = true;

        // Manually specify the file names
        int measuredSizes[] = new int[] { 1, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 2097152, 3145728, 4194304 };
        for (int size : measuredSizes)
            sizes.add((long) size);

        String fileNames[] = new String[measuredSizes.length];
        for (int i = 0; i < measuredSizes.length; i++)
            fileNames[i] = "get" + measuredSizes[i] + "B.csv";

        // Process E2E files
        processE2EFiles(INTER_REGIONS + "/latency_e2e/dram", fileNames, DataType.E2EIMC);
        processE2EFiles(INTER_REGIONS + "/latency_e2e/osc", fileNames, DataType.E2EOSC);
        processE2EFiles(INTER_REGIONS + "/latency_e2e/datalake", fileNames, DataType.E2EDL);

        // Process detailed files
        for (int i = 0; i < fileNames.length; i++)
            fileNames[i] = "get" + measuredSizes[i] + "B.csv";
        processDetailedFiles(INTER_REGIONS + "/latency_detailed", fileNames);

        /* Sanity check (all the lengths should be the same) */
        int sanityLength = -1;
        for (Map<DataType, List<Long>> latencyMap : e2eLats.values()) {
            for (List<Long> latencies : latencyMap.values()) {
                sanityLength = (sanityLength == -1) ? latencies.size() : sanityLength;
                assert sanityLength == latencies.size();
            }
        }
        for (Map<DataType, List<Long>> latencyMap : detailedLats.values()) {
            for (List<Long> latencies : latencyMap.values()) {
                sanityLength = (sanityLength == -1) ? latencies.size() : sanityLength;
                assert sanityLength == latencies.size();
            }
        }

        /* Start setting up latencies */
        if (cacheEnabled) { /* if macaron cache is enabled */
            /* Between App and Proxy */
            appProxyLatencyIMC = new RequestLatency();
            appProxyLatencyOSC = new RequestLatency();
            appProxyLatencyDL = new RequestLatency();
            for (long datasize : e2eLats.keySet()) {
                long[] a2pIMCLats = new long[sanityLength], a2pOSCLats = new long[sanityLength],
                        a2pDLLats = new long[sanityLength];
                Map<DataType, List<Long>> e2eLatMap = e2eLats.get(datasize);
                Map<DataType, List<Long>> p2pLatMap = detailedLats.get(datasize);
                for (int i = 0; i < sanityLength; i++) {
                    a2pIMCLats[i] = e2eLatMap.get(DataType.E2EIMC).get(i) - p2pLatMap.get(DataType.P2PIMC).get(i);
                    a2pOSCLats[i] = e2eLatMap.get(DataType.E2EOSC).get(i) - p2pLatMap.get(DataType.P2POSC).get(i);
                    a2pDLLats[i] = e2eLatMap.get(DataType.E2EDL).get(i) - p2pLatMap.get(DataType.P2PDL).get(i);
                }
                appProxyLatencyIMC.addData(datasize, a2pIMCLats);
                appProxyLatencyOSC.addData(datasize, a2pOSCLats);
                appProxyLatencyDL.addData(datasize, a2pDLLats);
            }
            appProxyLatencyIMC.setup();
            appProxyLatencyOSC.setup();
            appProxyLatencyDL.setup();
            appProxyLatencyIMC.setAvgSlope();
            appProxyLatencyOSC.setAvgSlope(appProxyLatencyDL.regSlope);
            appProxyLatencyDL.setAvgSlope(appProxyLatencyOSC.regSlope);

            /* Between Proxy and IMC */
            proxyIMCLatency = new RequestLatency();
            for (long datasize : detailedLats.keySet()) {
                long[] latencies = detailedLats.get(datasize).get(DataType.IMC).stream().mapToLong(l -> l).toArray();
                proxyIMCLatency.addData(datasize, latencies);
            }
            proxyIMCLatency.setup();
            proxyIMCLatency.setAvgSlope();

            /* Between Proxy and OSCM */
            proxyOSCMLatency = new RequestLatency();
            for (long datasize : detailedLats.keySet()) {
                long[] latencies = detailedLats.get(datasize).get(DataType.OSCM).stream().mapToLong(l -> l).toArray();
                proxyOSCMLatency.addData(datasize, latencies);
            }
            proxyOSCMLatency.setup();
            proxyOSCMLatency.setAvgSlope();

            /* Between Proxy and OSC */
            proxyOSCLatency = new RequestLatency();
            for (long datasize : detailedLats.keySet()) {
                long[] latencies = detailedLats.get(datasize).get(DataType.OSC).stream().mapToLong(l -> l).toArray();
                proxyOSCLatency.addData(datasize, latencies);
            }
            proxyOSCLatency.setup();

            /* Between Proxy and DL */
            proxyDLLatency = new RequestLatency();
            for (long datasize : detailedLats.keySet()) {
                long[] latencies = detailedLats.get(datasize).get(DataType.DL).stream().mapToLong(l -> l).toArray();
                proxyDLLatency.addData(datasize, latencies);
            }
            proxyDLLatency.setup();

            proxyOSCLatency.setAvgSlope(proxyDLLatency.regSlope);
            proxyDLLatency.setAvgSlope(proxyOSCLatency.regSlope);

            /* Between OSCM and OSC */
            oscmOSCLatency = new RequestLatency();
            for (long datasize : detailedLats.keySet()) {
                long[] latencies = detailedLats.get(datasize).get(DataType.OSC).stream().mapToLong(l -> l).toArray();
                oscmOSCLatency.addData(datasize, latencies);
            }
            oscmOSCLatency.setup();
            oscmOSCLatency.setAvgSlope();
        } else if (IS_LOCAL_DATALAKE) {
            /* Between App and Local DL */
            appDLLatency = new RequestLatency();
            for (long datasize : detailedLats.keySet()) {
                long[] latencies = detailedLats.get(datasize).get(DataType.OSC).stream().mapToLong(l -> l).toArray();
                appDLLatency.addData(datasize, latencies);
            }
            appDLLatency.setup();
            appDLLatency.setAvgSlope();
        } else {
            /* Between App and Remote DL */
            appDLLatency = new RequestLatency();
            for (long datasize : detailedLats.keySet()) {
                long[] latencies = detailedLats.get(datasize).get(DataType.DL).stream().mapToLong(l -> l).toArray();
                appDLLatency.addData(datasize, latencies);
            }
            appDLLatency.setup();
            appDLLatency.setAvgSlope();
        }
    }

    public long get(final CompType from, final CompType to, final long objSize, final CompType src) {
        assert from == CompType.ENGINE && to == CompType.APP;
        if (!IS_DRAM_ENABLED)
            return 0L;
        if (src == CompType.DRAM) {
            return appProxyLatencyIMC.getLatency(objSize);
        } else if (src == CompType.OSC) {
            return appProxyLatencyOSC.getLatency(objSize);
        } else if (src == CompType.DATALAKE) {
            return appProxyLatencyDL.getLatency(objSize);
        } else {
            throw new RuntimeException("Unexpected src type: " + src.toString());
        }
    }

    public long get(final CompType from, final CompType to, final long objSize) {
        if (from == CompType.APP) {
            if (to == CompType.ENGINE) {
                return 0L;
            } else if (to == CompType.DATALAKE) {
                return 0L;
            } else {
                throw new RuntimeException("Unexpected to CompType: " + to.toString());
            }
        } else if (from == CompType.ENGINE) {
            if (to == CompType.OSCM) {
                return 0L;
            } else if (to == CompType.OSC) {
                return 0L;
            } else if (to == CompType.DATALAKE) {
                return 0L;
            } else {
                throw new RuntimeException("Unexpected to CompType: " + to.toString());
            }
        } else if (from == CompType.DRAM) {
            if (to == CompType.ENGINE) {
                return proxyIMCLatency.getLatency(objSize);
            } else {
                throw new RuntimeException("Unexpected to CompType: " + to.toString());
            }
        } else if (from == CompType.OSCM) {
            if (to == CompType.ENGINE) {
                return proxyOSCMLatency.getLatency(1L);
            } else if (to == CompType.OSC) {
                return 0L;
            } else {
                throw new RuntimeException("Unexpected to CompType: " + to.toString());
            }
        } else if (from == CompType.OSC) {
            if (to == CompType.ENGINE) {
                return proxyOSCLatency.getLatency(objSize);
            } else if (to == CompType.OSCM) {
                return oscmOSCLatency.getLatency(objSize);
            } else {
                throw new RuntimeException("Unexpected to CompType: " + to.toString());
            }
        } else if (from == CompType.DATALAKE) {
            if (to == CompType.ENGINE) {
                return proxyDLLatency.getLatency(objSize);
            }
            if (to == CompType.APP) {
                return appDLLatency.getLatency(objSize);
            } else {
                throw new RuntimeException("Unexpected to CompType: " + to.toString());
            }
        } else {
            throw new RuntimeException("Unexpected from CompType: " + from.toString());
        }
    }

    public static void waitDone() {
        Logger.getGlobal().info("Waiting for gracefully finishing the latency generator...");
        if (appProxyLatencyIMC != null)
            appProxyLatencyIMC.waitDone();
        if (appProxyLatencyOSC != null)
            appProxyLatencyOSC.waitDone();
        if (appProxyLatencyDL != null)
            appProxyLatencyDL.waitDone();
        if (proxyIMCLatency != null)
            proxyIMCLatency.waitDone();
        if (proxyOSCMLatency != null)
            proxyOSCMLatency.waitDone();
        if (proxyOSCLatency != null)
            proxyOSCLatency.waitDone();
        if (proxyDLLatency != null)
            proxyDLLatency.waitDone();
        if (oscmOSCLatency != null)
            oscmOSCLatency.waitDone();
        if (appDLLatency != null)
            appDLLatency.waitDone();
    }

    // Start of the validation functions

    public Map<Long, Map<String, List<Long>>> validateProxyIMC() {
        Map<Long, Map<String, List<Long>>> ret = new HashMap<>();
        for (long datasize : e2eLats.keySet()) {
            ret.put(datasize, new HashMap<>());
            List<Long> measured = detailedLats.get(datasize).get(DataType.IMC);
            List<Long> generated = new ArrayList<>();
            for (int i = 0; i < measured.size(); i++)
                generated.add(proxyIMCLatency.getLatency(datasize));
            ret.get(datasize).put("measured", measured);
            ret.get(datasize).put("generated", generated);
        }
        return ret;
    }

    public Map<Long, Map<String, List<Long>>> validateProxyOSC() {
        Map<Long, Map<String, List<Long>>> ret = new HashMap<>();
        for (long datasize : e2eLats.keySet()) {
            ret.put(datasize, new HashMap<>());
            List<Long> measured = detailedLats.get(datasize).get(DataType.OSC);
            List<Long> generated = new ArrayList<>();
            for (int i = 0; i < measured.size(); i++)
                generated.add(proxyOSCLatency.getLatency(datasize));
            ret.get(datasize).put("measured", measured);
            ret.get(datasize).put("generated", generated);
        }
        return ret;
    }

    public Map<Long, Map<String, List<Long>>> validateProxyOSCM() {
        Map<Long, Map<String, List<Long>>> ret = new HashMap<>();
        for (long datasize : e2eLats.keySet()) {
            ret.put(datasize, new HashMap<>());
            List<Long> measured = detailedLats.get(datasize).get(DataType.OSCM);
            List<Long> generated = new ArrayList<>();
            for (int i = 0; i < measured.size(); i++)
                generated.add(proxyOSCMLatency.getLatency(datasize));
            ret.get(datasize).put("measured", measured);
            ret.get(datasize).put("generated", generated);
        }
        return ret;
    }

    public Map<Long, Map<String, List<Long>>> validateProxyDL() {
        Map<Long, Map<String, List<Long>>> ret = new HashMap<>();
        for (long datasize : e2eLats.keySet()) {
            ret.put(datasize, new HashMap<>());
            List<Long> measured = detailedLats.get(datasize).get(DataType.DL);
            List<Long> generated = new ArrayList<>();
            for (int i = 0; i < measured.size(); i++)
                generated.add(proxyDLLatency.getLatency(datasize));
            ret.get(datasize).put("measured", measured);
            ret.get(datasize).put("generated", generated);
        }
        return ret;
    }

    public Map<Long, Map<String, List<Long>>> validateE2EIMC() {
        Map<Long, Map<String, List<Long>>> ret = new HashMap<>();
        for (long datasize : e2eLats.keySet()) {
            ret.put(datasize, new HashMap<>());
            List<Long> measured = e2eLats.get(datasize).get(DataType.E2EIMC);
            List<Long> generated = new ArrayList<>();
            for (int i = 0; i < measured.size(); i++) {
                long genLat = appProxyLatencyIMC.getLatency(datasize) + proxyIMCLatency.getLatency(datasize);
                generated.add(genLat);
            }
            ret.get(datasize).put("measured", measured);
            ret.get(datasize).put("generated", generated);
        }
        return ret;
    }

    public Map<Long, Map<String, List<Long>>> validateE2EOSC() {
        Map<Long, Map<String, List<Long>>> ret = new HashMap<>();
        for (long datasize : e2eLats.keySet()) {
            ret.put(datasize, new HashMap<>());
            List<Long> measured = e2eLats.get(datasize).get(DataType.E2EOSC);
            List<Long> generated = new ArrayList<>();
            for (int i = 0; i < measured.size(); i++) {
                long genLat = appProxyLatencyOSC.getLatency(datasize) + proxyIMCLatency.getLatency(0L)
                        + proxyOSCMLatency.getLatency(datasize) + proxyOSCLatency.getLatency(datasize);
                generated.add(genLat);
            }
            ret.get(datasize).put("measured", measured);
            ret.get(datasize).put("generated", generated);
        }
        return ret;
    }

    public Map<Long, Map<String, List<Long>>> validateE2EDL() {
        Map<Long, Map<String, List<Long>>> ret = new HashMap<>();
        for (long datasize : e2eLats.keySet()) {
            ret.put(datasize, new HashMap<>());
            List<Long> measured = e2eLats.get(datasize).get(DataType.E2EDL);
            List<Long> generated = new ArrayList<>();
            for (int i = 0; i < measured.size(); i++) {
                long genLat = appProxyLatencyDL.getLatency(datasize) + proxyIMCLatency.getLatency(0L)
                        + proxyOSCMLatency.getLatency(0L) + proxyDLLatency.getLatency(datasize);
                generated.add(genLat);
            }
            ret.get(datasize).put("measured", measured);
            ret.get(datasize).put("generated", generated);
        }
        return ret;
    }

    public long getE2ELatencyGenerated(CompType compType, long objSize) {
        if (compType == CompType.DRAM) {
            return appProxyLatencyIMC.getLatency(objSize) + proxyIMCLatency.getLatency(objSize);
        } else if (compType == CompType.OSC) {
            return appProxyLatencyOSC.getLatency(objSize) + proxyIMCLatency.getLatency(0L)
                    + proxyOSCMLatency.getLatency(objSize) + proxyOSCLatency.getLatency(objSize);
        } else if (compType == CompType.DATALAKE) {
            return appProxyLatencyDL.getLatency(objSize) + proxyIMCLatency.getLatency(0L)
                    + proxyOSCMLatency.getLatency(0L) + proxyDLLatency.getLatency(objSize);
        } else {
            throw new RuntimeException("Unexpected CompType in getE2ELatency: " + compType.toString());
        }
    }

    public long getE2ELatency(CompType compType, long objSize) {
        assert(objSize > 0L);
        
        DataType dataType = compType == CompType.DATALAKE ? DataType.E2EDL : compType == CompType.OSC ? DataType.E2EOSC : DataType.E2EIMC;
        long latency = 0L;
        if (objSize == 1L) {
            latency = e2eAvgLats.get(1L).get(dataType);
        } else if (objSize <= sizes.get(sizes.size() - 1)) {
            for (int i = 1; i < sizes.size(); i++) {
                if (objSize <= sizes.get(i)) {
                    long lower = sizes.get(i - 1), upper = sizes.get(i);
                    long lowerLat = e2eAvgLats.get(lower).get(dataType), upperLat = e2eAvgLats.get(upper).get(dataType);
                    double slope = (double) (upperLat - lowerLat) / (double) (upper - lower);
                    latency = (long) (lowerLat + slope * (objSize - lower));
                    break;
                }
            }
        } else {
            throw new RuntimeException("Unexpected objSize: " + objSize);
        }

        assert(latency > 0L);
        return latency;
    }

    public double getAvgE2ELatency(CompType compType, long objSize) {
        long ret = 0L;
        for (int i = 0; i < 1000; i++)
            ret += getE2ELatency(compType, objSize);
        return (double) ret / 1000.;
    }

    // End of validation functions

    // Start of the file parser related functions 
    private static void processE2EFiles(String resourceFolder, String fileNames[], DataType dataType) {
        for (String fileName : fileNames) {
            long dataSize = extractLong(fileName);
            String resourcePath = resourceFolder + "/" + fileName;
            List<Long> latencies = readE2EFile(resourcePath);
            e2eLats.putIfAbsent(dataSize, new HashMap<>());
            e2eLats.get(dataSize).put(dataType, latencies);

            List<Long> newLatencies = new ArrayList<>(latencies);
            newLatencies.sort(Comparator.naturalOrder());
            long median = newLatencies.get(newLatencies.size() / 2);
            long sum = 0L;
            for (long lat : newLatencies)
                sum += lat;
            long avg = sum / newLatencies.size();
            e2eMedLats.putIfAbsent(dataSize, new HashMap<>());
            e2eMedLats.get(dataSize).put(dataType, median);
            e2eAvgLats.putIfAbsent(dataSize, new HashMap<>());
            e2eAvgLats.get(dataSize).put(dataType, avg);
        }
    }

    private static List<Long> readE2EFile(final String resourcePath) {
        List<Long> ret = new ArrayList<>();
        try (InputStream is = GammaDistributionWrapper.class.getClassLoader().getResourceAsStream(resourcePath);
                BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line;
            int lineCnt = 1;
            while ((line = reader.readLine()) != null) {
                if (lineCnt++ == 1) {
                    assert line.startsWith("key");
                } else {
                    String[] splits = line.split(",");
                    ret.add(Long.parseLong(splits[1].strip()));
                }
            }
        } catch (NumberFormatException | IOException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    private static void processDetailedFiles(String resourceFolder, String fileNames[]) {
        for (String fileName : fileNames) {
            long dataSize = extractLong(fileName);
            Map<DataType, List<Long>> latencies = readDetailedFile(resourceFolder + "/" + fileName);
            detailedLats.put(dataSize, latencies);
        }
    }

    private static Map<DataType, List<Long>> readDetailedFile(final String resourcePath) {
        Map<DataType, List<Long>> info = new HashMap<>();
        try (InputStream is = GammaDistributionWrapper.class.getClassLoader().getResourceAsStream(resourcePath);
                CSVReader reader = new CSVReader(new InputStreamReader(is))) {
            List<String[]> lines = reader.readAll();

            List<Long> dramLatencies = new ArrayList<>();
            List<Long> oscmLatencies = new ArrayList<>();
            List<Long> oscLatencies = new ArrayList<>();
            List<Long> dlLatencies = new ArrayList<>();

            List<Long> p2pIMCLatencies = new ArrayList<>();
            List<Long> p2pOSCLatencies = new ArrayList<>();
            List<Long> p2pDLLatencies = new ArrayList<>();

            for (String[] line : lines) {
                if (line[0].equals("key"))
                    continue;

                int src = Integer.parseInt(line[1]);
                long dramLatency = Long.parseLong(line[3]), oscmLatency = Long.parseLong(line[4]),
                        oscLatency = Long.parseLong(line[5]), dlLatency = Long.parseLong(line[6]);
                switch (src) {
                    case 0: // from DRAM
                        dramLatencies.add(dramLatency);
                        p2pIMCLatencies.add(dramLatency);
                        break;

                    case 1: // from OSC
                        oscmLatencies.add(oscmLatency);
                        oscLatencies.add(oscLatency);
                        p2pOSCLatencies.add(dramLatency + oscmLatency + oscLatency);
                        break;

                    case 2: // from DL
                        dlLatencies.add(dlLatency);
                        p2pDLLatencies.add(dramLatency + oscmLatency + oscLatency + dlLatency);
                        break;

                    default:
                        throw new RuntimeException("Unexpected source");
                }
            }

            info.put(DataType.IMC, dramLatencies);
            info.put(DataType.OSCM, oscmLatencies);
            info.put(DataType.OSC, oscLatencies);
            info.put(DataType.DL, dlLatencies);

            info.put(DataType.P2PIMC, p2pIMCLatencies);
            info.put(DataType.P2POSC, p2pOSCLatencies);
            info.put(DataType.P2PDL, p2pDLLatencies);
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }

        return info;
    }

    private static long extractLong(String filename) {
        String numericValue = filename.replaceAll("[^0-9]", "");
        return Long.parseLong(numericValue);
    }

    // End of the file parser related functions 
}
