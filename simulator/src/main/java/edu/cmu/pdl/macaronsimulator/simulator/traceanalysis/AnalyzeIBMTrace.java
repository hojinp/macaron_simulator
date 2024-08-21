package edu.cmu.pdl.macaronsimulator.simulator.traceanalysis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.util.Pair;
import edu.cmu.pdl.macaronsimulator.common.CustomLoggerGetter;
import edu.cmu.pdl.macaronsimulator.simulator.message.QType;
import edu.cmu.pdl.macaronsimulator.simulator.message.TraceQueryTypeMap;
import edu.cmu.pdl.macaronsimulator.simulator.tracedb.TraceDataDB;

public class AnalyzeIBMTrace {
    public static void main(String[] args) throws IOException, ParseException {
        Logger logger = CustomLoggerGetter.getCustomLogger();
        logger.info("AnalyzeIBMTrace:Start running AnalyzeIBMTrace");

        Options options = new Options();
        options.addRequiredOption("f", "TraceFilename", true, "Trace filename to analyze");
        options.addRequiredOption("d", "DirnameForOutput", true, "Directory name for the output information");
        options.addRequiredOption("p", "tracedbfilePath", true, "Path to the trace DB file");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        final String traceFilename = cmd.getOptionValue("f");
        final String outputDirname = cmd.getOptionValue("d");
        assert Files.exists(Paths.get(traceFilename)) && !Files.exists(Paths.get(outputDirname));
        Files.createDirectories(Paths.get(outputDirname));

        logger.info("* Trace filename: " + traceFilename);
        logger.info("* Output dirname: " + outputDirname);
        logger.info("* Start reading trace file");

        TraceDataDB.traceFilePath = traceFilename;
        TraceDataDB.dbFilePath = cmd.getOptionValue("p");
        TraceDataDB.initialize();

        final long HR_US = 60L * 60L * 1000000L;
        long profile_us = HR_US, timestamp = 0L;
        int putCnt, getCnt, deleteCnt;
        putCnt = getCnt = deleteCnt = 0;
        Map<Integer, List<Integer>> hrToOpCnt = new HashMap<>(); // put, get, delete counts
        long putBytes, getBytes;
        putBytes = getBytes = 0L;
        Map<Integer, Pair<Long, Long>> hrToBytes = new HashMap<>(); // put, get bytes

        /** 
         * In the first pass, do the followings:
         * 1. Get the MaxDecodedOid value
         * 2. Get the total working set size (create data lake from the trace)
        */
        Map<Integer, Long> datalake = new HashMap<>();
        Map<Integer, Boolean> seen = new HashMap<>();
        Map<Integer, Long> hrToWss = new HashMap<>();
        Map<Integer, Long> hrToWssNoDel = new HashMap<>();
        Map<Integer, Integer> oidToCount = new HashMap<>();
        long currWss = 0L, totalWss = 0L;
        long currWssNoDel = 0L, totalWssNoDel = 0L;
        long totalNumberOfRequests = 0L;

        TraceDataDB.prepareNextRequest();
        while (TraceDataDB.getNextRequest()) {
            timestamp = TraceDataDB.seqTimestamp;
            if (timestamp > profile_us) {
                while (timestamp > profile_us) {
                    int hr = (int) (profile_us / HR_US);
                    hrToWss.put(hr, currWss);
                    hrToWssNoDel.put(hr, currWssNoDel);
                    profile_us += HR_US;
                }
            }

            final int typeIdx = TraceDataDB.seqOpType;
            final QType queryType = TraceQueryTypeMap.get(typeIdx);
            final int objectId = TraceDataDB.seqObjectId;
            if (queryType == QType.GET || queryType == QType.PUT) {
                final long objSize = TraceDataDB.seqObjectSize;
                assert objSize > 0L;
                long oldObjSize = datalake.getOrDefault(objectId, 0L);
                if (oldObjSize != objSize) {
                    currWss += (objSize - oldObjSize);
                    datalake.put(objectId, objSize);
                }

                boolean exists = seen.getOrDefault(objectId, false);
                if (!exists) {
                    currWssNoDel += objSize;
                    seen.put(objectId, true);
                }
            } else {
                if (datalake.containsKey(objectId)) {
                    long oldObjSize = datalake.remove(objectId);
                    currWss -= oldObjSize;
                }
            }

            if (queryType == QType.GET) {
                oidToCount.put(objectId, oidToCount.getOrDefault(objectId, 0) + 1);
            }
            totalNumberOfRequests += 1L;
        }
        {
            int hr = (int) (profile_us / HR_US);
            hrToWss.put(hr, currWss);
            hrToWssNoDel.put(hr, currWssNoDel);
            totalWss = currWss;
            totalWssNoDel = currWssNoDel;
        }

        /* Write the data access count distribution to the file */
        String outputFilename = Paths.get(outputDirname, "rank_to_data_access_count.log").toString();
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilename));
        writer.write("Rank,DataAccessCount\n");
        int rank = 1;
        List<Integer> counts = new ArrayList<>(oidToCount.values());
        Collections.sort(counts);
        Collections.reverse(counts);
        for (int count : counts) {
            writer.write(String.format("%d,%d\n", rank++, count));
        }
        writer.close();

        outputFilename = Paths.get(outputDirname, "access_counts.csv").toString();
        writer = new BufferedWriter(new FileWriter(outputFilename));
        writer.write("DataAccessCount,Number\n");
        Map<Integer, Integer> dataAccessCountToCount = new TreeMap<>();
        for (int count : oidToCount.values()) {
            dataAccessCountToCount.put(count, dataAccessCountToCount.getOrDefault(count, 0) + 1);
        }
        for (int count : dataAccessCountToCount.keySet()) {
            writer.write(String.format("%d,%d\n", count, dataAccessCountToCount.get(count)));
        }
        writer.close();

        final long kbToBytes = 1024L;
        final long mbToBytes = 1024L * 1024L;
        final long gbToBytes = 1024L * 1024L * 1024L;

        Set<Integer> oneTimeAccess = new HashSet<>();
        List<Long> reuseDists = new ArrayList<>();
        Map<Integer, Long> lastUsedTs = new HashMap<>();
        long profileRequestCount = totalNumberOfRequests / 10;
        long currRequestCount = 0L;

        profile_us = HR_US;
        timestamp = 0L;

        TraceDataDB.prepareNextRequest();
        while (TraceDataDB.getNextRequest()) {
            timestamp = TraceDataDB.seqTimestamp;
            if (timestamp > profile_us) {
                while (timestamp > profile_us) {
                    int hr = (int) (profile_us / HR_US);
                    assert !hrToOpCnt.containsKey(hr) && !hrToBytes.containsKey(hr);
                    hrToOpCnt.put(hr, new ArrayList<>(Arrays.asList(putCnt, getCnt, deleteCnt)));
                    hrToBytes.put(hr, new Pair<>(putBytes, getBytes));
                    putCnt = getCnt = deleteCnt = 0;
                    putBytes = getBytes = 0L;
                    profile_us += HR_US;
                }
            }
            final int typeIdx = TraceDataDB.seqOpType;
            final QType queryType = TraceQueryTypeMap.get(typeIdx);
            final int objectId = TraceDataDB.seqObjectId;
            if (queryType == QType.GET) {
                getCnt++;
                final long objectSize = TraceDataDB.seqObjectSize;
                assert objectSize > 0L;

                getBytes += objectSize;

                if (!lastUsedTs.containsKey(objectId)) {
                    oneTimeAccess.add(objectId);
                    lastUsedTs.put(objectId, timestamp);
                } else {
                    assert lastUsedTs.containsKey(objectId);
                    oneTimeAccess.remove(objectId);
                    long reuseDist = timestamp - lastUsedTs.get(objectId);
                    reuseDists.add(reuseDist);
                    lastUsedTs.put(objectId, timestamp);
                }
            } else if (queryType == QType.PUT) {
                putCnt++;
                final long objectSize = TraceDataDB.seqObjectSize;
                assert objectSize > 0L;
                putBytes += objectSize;
            } else if (queryType == QType.DELETE) {
                deleteCnt++;
            } else {
                assert false : "Unexpected query type in Application: " + queryType.toString();
            }
            currRequestCount += 1L;
            if (currRequestCount > profileRequestCount) {
                profileRequestCount += totalNumberOfRequests / 10;
                logger.info(String.format("Progress..... (%d/%d)", currRequestCount, totalNumberOfRequests));
            }
        }
        {
            int hr = (int) (profile_us / HR_US);
            assert !hrToOpCnt.containsKey(hr) && !hrToBytes.containsKey(hr);
            hrToOpCnt.put(hr, new ArrayList<>(Arrays.asList(putCnt, getCnt, deleteCnt)));
            hrToBytes.put(hr, new Pair<>(putBytes, getBytes));
        }
        final int lastHr = (int) (profile_us / HR_US);

        // 1. Start analyzing data distribution
        List<Long> cdfXList = new ArrayList<>(Arrays.asList(8L, 16L, 32L, 64L, 128L, 256L, 512L, (1 * kbToBytes),
                (2 * kbToBytes), (4 * kbToBytes), (8 * kbToBytes), (16 * kbToBytes), (32 * kbToBytes), (64 * kbToBytes),
                (128 * kbToBytes), (256 * kbToBytes), (512 * kbToBytes), (1 * mbToBytes), (2 * mbToBytes),
                (3 * mbToBytes), (4 * mbToBytes), (5 * mbToBytes), (10 * mbToBytes), (gbToBytes)));
        Map<Long, Double> cdfXtoY = new HashMap<>();
        int cdfIdx = 0;
        List<Long> objectSizes = new ArrayList<>(datalake.values());
        Collections.sort(objectSizes);
        ListIterator<Long> it = objectSizes.listIterator();
        while (it.hasNext()) {
            int index = it.nextIndex();
            long objectSize = it.next();
            while (objectSize > cdfXList.get(cdfIdx)) {
                int cnt = index + 1;
                double cdf = (double) cnt / objectSizes.size();
                cdfXtoY.put(cdfXList.get(cdfIdx), cdf);
                cdfIdx++;
                assert cdfIdx < cdfXList.size();
            }
        }
        while (cdfIdx < cdfXList.size()) {
            cdfXtoY.put(cdfXList.get(cdfIdx), 1.0);
            cdfIdx++;
        }
        outputFilename = Paths.get(outputDirname, "data_sizes.csv").toString();
        writer = new BufferedWriter(new FileWriter(outputFilename));
        writer.write("ObjectSize,CDF\n");
        for (long cdfX : cdfXList) {
            writer.write(String.format("%d,%.3f\n", cdfX, cdfXtoY.get(cdfX)));
        }
        writer.close();

        for (int hr = 1; hr <= lastHr; hr++) {
            assert hrToOpCnt.containsKey(hr) && hrToBytes.containsKey(hr);
        }

        // 3. Start writing op count and bytes per hour
        outputFilename = Paths.get(outputDirname, "load.csv").toString();
        writer = new BufferedWriter(new FileWriter(outputFilename));
        writer.write("HR,PutCount,GetCount,DeleteCount,PutBytes,GetBytes\n");
        for (int hr = 1; hr <= lastHr; hr++) {
            List<Integer> opCnt = hrToOpCnt.get(hr);
            Pair<Long, Long> bytes = hrToBytes.get(hr);
            writer.write(String.format("%d,%d,%d,%d,%d,%d\n", hr, opCnt.get(0), opCnt.get(1), opCnt.get(2),
                    bytes.getKey(), bytes.getValue()));
        }
        writer.close();

        // 4. Start writing summary of each trace
        outputFilename = Paths.get(outputDirname, "summary.log").toString();
        writer = new BufferedWriter(new FileWriter(outputFilename));
        writer.write(String.format(
                "TraceId,ValueSize(KB),RequestRate(rph),TotalNumberOfRequests,NumberOfUniqueKeys,PutRatio,GetRatio,DeleteRatio,TotalWSS,TotalWSSNoDel\n"));
        String traceId = Paths.get(traceFilename).getFileName().toString();
        double avgObjectSize = 0.;
        for (long objectSize : objectSizes)
            avgObjectSize += (double) objectSize / objectSizes.size();
        avgObjectSize /= 1000.; // Convert to KB
        totalNumberOfRequests = 0;
        putCnt = 0;
        getCnt = 0;
        deleteCnt = 0;
        for (List<Integer> opCnt : hrToOpCnt.values()) {
            totalNumberOfRequests += (opCnt.get(0) + opCnt.get(1) + opCnt.get(2));
            putCnt += opCnt.get(0);
            getCnt += opCnt.get(1);
            deleteCnt += opCnt.get(2);
        }
        int requestRate = (int) (totalNumberOfRequests / hrToOpCnt.size());
        int uniqueKeyCount = datalake.size();
        writer.write(String.format("%s,%.2f,%d,%d,%d,%.2f,%.2f,%.2f,%d,%d\n", traceId, avgObjectSize, requestRate,
                totalNumberOfRequests, uniqueKeyCount, (double) putCnt / totalNumberOfRequests,
                (double) getCnt / totalNumberOfRequests, (double) deleteCnt / totalNumberOfRequests, totalWss,
                totalWssNoDel));
        writer.close();

        // 5. Start writing WSS size
        outputFilename = Paths.get(outputDirname, "wss.csv").toString();
        writer = new BufferedWriter(new FileWriter(outputFilename));
        writer.write(String.format("HR,WSS\n"));
        for (int hr = 1; hr <= lastHr; hr++)
            writer.write(String.format("%d,%d\n", hr, hrToWss.get(hr)));
        writer.close();

        outputFilename = Paths.get(outputDirname, "wss_no_del.csv").toString();
        writer = new BufferedWriter(new FileWriter(outputFilename));
        writer.write(String.format("HR,WSS\n"));
        for (int hr = 1; hr <= lastHr; hr++)
            writer.write(String.format("%d,%d\n", hr, hrToWssNoDel.get(hr)));
        writer.close();

        // 6. Start writing reuse distance info
        outputFilename = Paths.get(outputDirname, "reuse_distance").toString();
        writer = new BufferedWriter(new FileWriter(outputFilename));
        writer.write(String.format("Number of total accessed objects: %d\n", lastUsedTs.size()));
        writer.write(String.format("Number of objects that are accessed only once: %d\n", oneTimeAccess.size()));
        Collections.sort(reuseDists);
        long timeBlock = 30L * 60L * 1000000L; // 30 min
        long currTimeBlock = timeBlock;
        writer.write(String.format("cdf [reuse dist (min), percentage]\n"));
        writer.write(String.format("0,0.\n"));
        for (int i = 0; i < reuseDists.size(); i++) {
            if (reuseDists.get(i) > currTimeBlock) {
                writer.write(String.format("%d,%.3f\n", (int) (currTimeBlock / 60L / 1000000L),
                        (float) i / reuseDists.size()));
                currTimeBlock += timeBlock;
                i--;
            }
        }
        writer.write(String.format("%d,1.0\n", (int) (currTimeBlock / 60L / 1000000L)));
        writer.close();
    }
}
