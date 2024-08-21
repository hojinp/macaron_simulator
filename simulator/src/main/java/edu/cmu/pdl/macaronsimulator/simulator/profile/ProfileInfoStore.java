package edu.cmu.pdl.macaronsimulator.simulator.profile;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.macaroncache.reconfig.ReconfigEvent;

public class ProfileInfoStore {
    public static String LOG_DIRNAME = null;
    private final static long MIN_US = 60L * 1000L * 1000L;
    private final Map<Integer, Map<CompType, List<Pair<String, ProfileInfo>>>> profileInfoCollection = new HashMap<>();

    /**
     * Add the profile info to the store
     * 
     * @param timestamp the timestamp of the profile info
     * @param componentType the component type of the profile info
     * @param name the name of the profile info
     * @param profileInfo the profile info to add
     */
    public void addProfileInfo(long timestamp, CompType componentType, String name, ProfileInfo profileInfo) {
        if (profileInfo == null)
            return;

        profileInfoCollection.computeIfAbsent((int) (timestamp / MIN_US), k -> new HashMap<>())
                .computeIfAbsent(componentType, k -> new ArrayList<>()).add(new Pair<>(name, profileInfo));
    }

    /**
     * Save the profile info to the file
     * 
     * @param timestamp the timestamp of the profile info
     */
    public void saveProfileInfosToFile(long timestamp) {
        if (LOG_DIRNAME == null)
            throw new RuntimeException("LOG_DIRNAME is not set");

        int curMin = (int) (timestamp / MIN_US);
        Map<CompType, List<Pair<String, ProfileInfo>>> typeToProfileInfos = profileInfoCollection.get(curMin);
        for (final CompType componentType : typeToProfileInfos.keySet()) {
            if ((ReconfigEvent.IS_DRAM_OPT_ENABLED || ReconfigEvent.DRAM_OPT_IS_MAIN) && (componentType == CompType.ENGINE || componentType == CompType.DRAM)) // ROLLBACK
                continue;

            for (final Pair<String, ProfileInfo> nameProfileInfo : typeToProfileInfos.get(componentType)) {
                String fileName = String.format("%s-%s.log", componentType.toString(), nameProfileInfo.getKey());
                Path filePath = Paths.get(LOG_DIRNAME, fileName);

                boolean needsWriteHeader = !Files.exists(filePath);
                try (BufferedWriter writer = Files.newBufferedWriter(filePath, StandardOpenOption.APPEND,
                        StandardOpenOption.CREATE)) {
                    if (needsWriteHeader)
                        writer.write(ProfileInfo.getHeaderStr());
                    writer.write(nameProfileInfo.getValue().toString(curMin));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Return the pair of the expected number of Get and Put requests for the next time window
     * 
     * @param curMin the hour to get the expected number of Get and Put requests
     * @return the pair of the expected number of Get and Put requests for the next time window
     */
    public Pair<Integer, Integer> getGetAndPutCnts(int curMin) {
        int statWindowMin = ReconfigEvent.getStatWindowMin(), begMin = 0;
        int timeWindowCnts = curMin - begMin < statWindowMin ? 1 : (curMin - begMin) / statWindowMin;
        if (curMin < begMin || timeWindowCnts < 1)
            throw new RuntimeException("Cannot get MRC result before " + begMin + " min or with <1 time window");

        int gets[] = new int[timeWindowCnts], puts[] = new int[timeWindowCnts];
        for (int i = 0; i < timeWindowCnts; i++) {
            int startMin = curMin - (i + 1) * statWindowMin, endMin = curMin - i * statWindowMin;
            for (int j = startMin + 1; j <= endMin; j++) {
                if (!profileInfoCollection.containsKey(j))
                    continue;

                for (Pair<String, ProfileInfo> nameProfileInfo : profileInfoCollection.get(j).get(CompType.ENGINE)) {
                    ProfileInfo pi = nameProfileInfo.getSecond();
                    gets[i] += pi.getGetCnt();
                    puts[i] += pi.getPutCnt();
                }
            }
        }

        double coefficients[] = ReconfigEvent.getDecayCoefficient(timeWindowCnts);
        int get = 0, put = 0;
        for (int i = 0; i < timeWindowCnts; i++) {
            get += gets[i] * coefficients[i];
            put += puts[i] * coefficients[i];
        }

        return new Pair<>(get, put);
    }

    /**
     * Return how much the block is occupied
     * 
     * @param curMin the timestamp (minute) to get the block occupancy
     * @return how much the block is occupied
     */
    public double getBlockEffectivePortion(int curMin) {
        long occupiedSize = profileInfoCollection.get(curMin).get(CompType.OSCM).get(0).getSecond().getOccupiedSize();
        long dataSize = profileInfoCollection.get(curMin).get(CompType.OSC).get(0).getSecond().getCapacity();
        if (dataSize == 0L || occupiedSize == 0L)
            return 1.;
        return (double) occupiedSize / dataSize;
    }

    /**
     * Return if the profile info is available at the given time
     * 
     * @param curMin the time (minute) to check the profile info
     * @return if the profile info is available at the given time
     */
    public boolean hasProfileInfoAtMin(int curMin) {
        return profileInfoCollection.containsKey(curMin);
    }
}
