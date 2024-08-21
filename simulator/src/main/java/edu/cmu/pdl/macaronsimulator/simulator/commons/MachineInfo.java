package edu.cmu.pdl.macaronsimulator.simulator.commons;

/**
 * Stores the information of a machine.
 */
public class MachineInfo {
    private String name;
    private Long capacity;
    private double ondemandPrice;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public double getOndemandPrice() {
        return ondemandPrice;
    }

    public void setOndemandPrice(double ondemandPrice) {
        this.ondemandPrice = ondemandPrice;
    }
}
