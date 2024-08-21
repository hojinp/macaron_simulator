package edu.cmu.pdl.macaronsimulator.simulator.profile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;

import edu.cmu.pdl.macaronsimulator.simulator.commons.CompType;
import edu.cmu.pdl.macaronsimulator.simulator.commons.MachineInfo;
import edu.cmu.pdl.macaronsimulator.simulator.commons.MachineInfoGetter;
import edu.cmu.pdl.macaronsimulator.simulator.commons.OSPriceInfo;
import edu.cmu.pdl.macaronsimulator.simulator.commons.OSPriceInfoGetter;

public class CostInfoStore {
    private final static long HR_US = 60L * 60L * 1000L * 1000L;
    private final static long MIN_US = 60L * 1000L * 1000L;
    private final Map<Integer, Map<CompType, List<Pair<String, CostInfo>>>> costInfoCollection = new HashMap<>();

    public void calculateAddCost(long timestamp, CompType componentType, String name, ProfileInfo profileInfo,
            String MAC_SERVER_MTYPE) {
        double oscPutCost = 0.0;
        double oscGetCost = 0.0;
        double oscGCPutCost = 0.0;
        double oscGCGetCost = 0.0;
        double dlPutCost = 0.0;
        double dlGetCost = 0.0;
        double capacityCost = 0.0; // OSC
        double recvCost = 0.0; // Datalake
        double sendCost = 0.0; // Datalake
        double machineCost = 0.0; // IMC
        final long gbToByte = 1024L * 1024L * 1024L;
        final OSPriceInfo oscCostInfo = OSPriceInfoGetter.getOSPriceInfo();

        switch (componentType) {
            case APP:
                return;
            case ENGINE:
                return;
            case DRAM:
                if (MAC_SERVER_MTYPE != null) {
                    final MachineInfo machineInfo = MachineInfoGetter.getMachineInfo(MAC_SERVER_MTYPE);
                    machineCost = machineInfo.getOndemandPrice() * ProfileEvent.profileInterval / HR_US;
                }
                break;
            case OSCM:
                return;
            case OSC:
                capacityCost = (double) profileInfo.getOccupiedSize() / gbToByte * oscCostInfo.getCapacity()
                        * ProfileEvent.profileInterval / HR_US;
                oscPutCost = profileInfo.getPutCnt() * oscCostInfo.getPut();
                oscGetCost = profileInfo.getGetCnt() * oscCostInfo.getGet();
                oscGCPutCost = profileInfo.getGCPutCnt() * oscCostInfo.getPut();
                oscGCGetCost = profileInfo.getGCGetCnt() * oscCostInfo.getGet();
                break;
            case DATALAKE:
                recvCost = (double) profileInfo.getRecvBytes() / gbToByte * oscCostInfo.getTransfer();
                sendCost = (double) profileInfo.getSendBytes() / gbToByte * oscCostInfo.getTransfer();
                dlPutCost = profileInfo.getPutCnt() * oscCostInfo.getPut();
                dlGetCost = profileInfo.getGetCnt() * oscCostInfo.getGet();
                break;
            case CONTROLLER:
                return;
            default:
                throw new RuntimeException("Unexpected component type: " + componentType.toString());
        }

        final CostInfo costInfo = new CostInfo(oscPutCost, oscGetCost, oscGCPutCost, oscGCGetCost, dlPutCost, dlGetCost,
                capacityCost, recvCost, sendCost, machineCost);

        addCostInfo(timestamp, componentType, name, costInfo);
    }

    public void addCostInfo(long timestamp, CompType compType, String name, CostInfo costInfo) {
        if (costInfo == null) {
            return;
        }
        final int minute = (int) (timestamp / MIN_US);
        if (!costInfoCollection.containsKey(minute)) {
            costInfoCollection.put(minute, new HashMap<>());
        }
        final Map<CompType, List<Pair<String, CostInfo>>> typeToCostInfos = costInfoCollection.get(minute);
        if (!typeToCostInfos.containsKey(compType)) {
            typeToCostInfos.put(compType, new ArrayList<>());
        }
        final List<Pair<String, CostInfo>> costInfos = typeToCostInfos.get(compType);
        costInfos.add(new Pair<String, CostInfo>(name, costInfo));
    }

    public void saveCostInfosToFile(final long timestamp) {
        assert ProfileInfoStore.LOG_DIRNAME != null;
        final int minute = (int) (timestamp / MIN_US);
        Map<CompType, List<Pair<String, CostInfo>>> typeToCostInfos = costInfoCollection.get(minute);
        File f = new File(Paths.get(ProfileInfoStore.LOG_DIRNAME, "cost").toString() + ".log");
        boolean exists = f.exists();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(f, true))) {
            if (!exists)
                writer.write(CostInfo.getHeaderStr());
            for (final CompType componentType : typeToCostInfos.keySet()) {
                for (final Pair<String, CostInfo> nameCostInfo : typeToCostInfos.get(componentType)) {
                    writer.write(componentType.toString() + "-" + nameCostInfo.getKey() + ","
                            + nameCostInfo.getValue().toString(minute));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
