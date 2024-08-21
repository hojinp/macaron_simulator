package edu.cmu.pdl.macaronsimulator.simulator.profile;

public class CostInfo {
    private final double oscPutCost;
    private final double oscGetCost;
    private final double oscGCPutCost;
    private final double oscGCGetCost;

    private final double dlPutCost;
    private final double dlGetCost;

    private final double capacityCost; // OSC
    private final double recvCost; // transfer to App (GB)
    private final double sendCost; // transfer to Datalake (GB)
    private final double machineCost; // VM

    public CostInfo(double oscPut, double oscGet, double oscGCPut, double oscGCGet, double dlPut, double dlGet,
            double capacity, double recv, double send, double machine) {
        oscPutCost = oscPut;
        oscGetCost = oscGet;
        oscGCPutCost = oscGCPut;
        oscGCGetCost = oscGCGet;

        dlPutCost = dlPut;
        dlGetCost = dlGet;

        capacityCost = capacity;
        recvCost = recv;
        sendCost = send;
        machineCost = machine;
    }

    public static String getHeaderStr() {
        return "Component,Time(min),PutOSC,GetOSC,GCPutOSC,GCGetOSC,PutDatalake,GetDatalake,Capacity,Transfer2App,Transfer2DL,Machine\n";
    }

    public String toString(int minute) {
        return String.format("%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n", minute, oscPutCost, oscGetCost, oscGCPutCost,
                oscGCGetCost, dlPutCost, dlGetCost, capacityCost, sendCost, recvCost, machineCost);
    }
}
