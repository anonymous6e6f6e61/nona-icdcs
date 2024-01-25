package util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class TimestampedStringLogger {

    private final PrintWriter out;
    private final TimeUnit timeUnit;

    public TimestampedStringLogger(String outputFile, boolean autoFlush, TimeUnit timeUnit) {
        FileWriter outFile;
        this.timeUnit = timeUnit;
        try {
            outFile = new FileWriter(outputFile);
            out = new PrintWriter(outFile, autoFlush);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void log(String message) {
        long currentTime;
        if (timeUnit == TimeUnit.SECONDS) {
            // time in seconds
            currentTime = Math.round((double) System.currentTimeMillis() / 1000);
        } else {
            // else, must be milliseconds
            currentTime = System.currentTimeMillis();
        }
        out.println(currentTime + "," + message);
    }

    public void flush() {
        out.flush();
    }

    public void close() {
        out.flush();
        out.close();
    }

    public enum TimeUnit {
        SECONDS, MILLISECONDS
    }
}
