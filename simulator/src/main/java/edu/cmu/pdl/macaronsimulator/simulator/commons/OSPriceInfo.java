package edu.cmu.pdl.macaronsimulator.simulator.commons;

/**
 * Cost information of an object store.
 */
public class OSPriceInfo {
    private double transfer;
    private double put;
    private double get;
    private double delete;
    private double capacity;

    public double getTransfer() {
        return transfer;
    }

    public void setTransfer(double transfer) {
        this.transfer = transfer;
    }

    public double getPut() {
        return put;
    }

    public void setPut(double put) {
        this.put = put;
    }

    public double getGet() {
        return get;
    }

    public void setGet(double get) {
        this.get = get;
    }

    public double getDelete() {
        return delete;
    }

    public void setDelete(double delete) {
        this.delete = delete;
    }

    public double getCapacity() {
        return capacity;
    }

    public void setCapacity(double capacity) {
        this.capacity = capacity;
    }
}