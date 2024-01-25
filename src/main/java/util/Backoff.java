package util;

import java.io.Serializable;

public class Backoff implements Serializable {

    private final long maxDurationMillis;
    private long currentDurationMillis = 1;

    public Backoff(long maxDurationMillis) {
        this.maxDurationMillis = maxDurationMillis;
    }

    public Backoff() {
        this.maxDurationMillis = Long.MAX_VALUE;
    }

    public void backoff() throws InterruptedException {
        Thread.sleep(currentDurationMillis);
        currentDurationMillis = Math.min(currentDurationMillis * 2, maxDurationMillis);
    }

    public void reset() {
        currentDurationMillis = 1;
    }
}
