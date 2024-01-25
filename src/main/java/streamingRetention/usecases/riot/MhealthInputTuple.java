package streamingRetention.usecases.riot;

import util.BaseTuple;

import java.util.Objects;
import java.util.regex.Pattern;

public class MhealthInputTuple extends BaseTuple {
    private static final Pattern DELIMITER_PATTERN = Pattern.compile(",");
    private double accelerationChestX;
    private double accelerationChestY;
    private double accelerationChestZ;
    private double ecgSignalLead1;
    private double ecgSignalLead2;

    public static MhealthInputTuple fromReading(String reading) {
        try {
            String[] tokens = DELIMITER_PATTERN.split(reading.trim());
            return new MhealthInputTuple(tokens);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "Failed to parse reading: %s", reading), e);
        }
    }

    protected MhealthInputTuple(String[] readings) {
        this(
                Long.parseLong(readings[0]),
                System.currentTimeMillis(),
                Double.parseDouble(readings[1]),
                Double.parseDouble(readings[2]),
                Double.parseDouble(readings[3]),
                Double.parseDouble(readings[4]),
                Double.parseDouble(readings[5]),
                Integer.parseInt(readings[6])
        );

    }

    public MhealthInputTuple(long timestamp, long stimulus, double accelerationChestX, double accelerationChestY, double accelerationChestZ, double ecgSignalLead1, double ecgSignalLead2, int activityLabel) {
        super(timestamp, String.valueOf(activityLabel), stimulus);
        this.accelerationChestX = accelerationChestX;
        this.accelerationChestY = accelerationChestY;
        this.accelerationChestZ = accelerationChestZ;
        this.ecgSignalLead1 = ecgSignalLead1;
        this.ecgSignalLead2 = ecgSignalLead2;
    }

    public double getAccelerationChestX() {
        return accelerationChestX;
    }

    public void setAccelerationChestX(double accelerationChestX) {
        this.accelerationChestX = accelerationChestX;
    }

    public double getAccelerationChestY() {
        return accelerationChestY;
    }

    public void setAccelerationChestY(double accelerationChestY) {
        this.accelerationChestY = accelerationChestY;
    }

    public double getAccelerationChestZ() {
        return accelerationChestZ;
    }

    public void setAccelerationChestZ(double accelerationChestZ) {
        this.accelerationChestZ = accelerationChestZ;
    }

    public double getEcgSignalLead1() {
        return ecgSignalLead1;
    }

    public void setEcgSignalLead1(double ecgSignalLead1) {
        this.ecgSignalLead1 = ecgSignalLead1;
    }

    public double getEcgSignalLead2() {
        return ecgSignalLead2;
    }

    public void setEcgSignalLead2(double ecgSignalLead2) {
        this.ecgSignalLead2 = ecgSignalLead2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MhealthInputTuple that = (MhealthInputTuple) o;
        return Double.compare(that.accelerationChestX, accelerationChestX) == 0 && Double.compare(that.accelerationChestY, accelerationChestY) == 0 && Double.compare(that.accelerationChestZ, accelerationChestZ) == 0 && Double.compare(that.ecgSignalLead1, ecgSignalLead1) == 0 && Double.compare(that.ecgSignalLead2, ecgSignalLead2) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), accelerationChestX, accelerationChestY, accelerationChestZ, ecgSignalLead1, ecgSignalLead2);
    }

    @Override
    public String toString() {
        return "MhealthInputTuple{" +
                "accelerationChestX=" + accelerationChestX +
                ", accelerationChestY=" + accelerationChestY +
                ", accelerationChestZ=" + accelerationChestZ +
                ", ecgSignalLead1=" + ecgSignalLead1 +
                ", ecgSignalLead2=" + ecgSignalLead2 +
                ", timestamp=" + timestamp +
                ", stimulus=" + stimulus +
                ", key='" + key + '\'' +
                "} " + super.toString();
    }
}
