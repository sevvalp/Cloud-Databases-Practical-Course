package de.tum.i13.Performance;

public class Performance {
    /**
     * time spent to execute in second
     */
    private double runtime;
    private long numOps;

    public Performance() {
        this.runtime = 0;
        this.numOps = 0;
    }

    /**
     * calculates how many operations can be executed in a ms
     * @return the average throughput (ops/ms)
     */
    public double getThroughput() {
        return ((double) numOps) /  runtime;
    }

    /**
     * calculates much time an operation need in average
     * @return the average latency (ms/op)
     */
    public double getLatency() {
        return runtime / ((double) numOps);
    }

    public double getRuntime() {
        return runtime;
    }

    public long getNumOps() {
        return numOps;
    }

    public Performance withRuntime(double runtime) {
        this.runtime = runtime;
        return this;
    }

    public Performance withNumOps(long numOps) {
        this.numOps = numOps;
        return this;
    }
}
