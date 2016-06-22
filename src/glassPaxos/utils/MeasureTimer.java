package glassPaxos.utils;

import java.text.DecimalFormat;

public class MeasureTimer {
    private long startTime = 0L;
    private long endTime = 0L;

    public void start() {
        this.startTime = System.nanoTime();
    }

    public void end() {
        this.endTime = System.nanoTime();
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getExecutionTime() {
        return (this.endTime - this.startTime)/1000000;
    }

    public double getPreciseExecutionTime() {
        DecimalFormat f = new DecimalFormat("##.000");
        String tm = f.format((this.endTime - this.startTime)/1000000.0);
        return Double.parseDouble(tm);
    }
}
