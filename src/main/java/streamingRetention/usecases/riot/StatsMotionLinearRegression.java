package streamingRetention.usecases.riot;


import ananke.aggregate.ProvenanceAggregateStrategy;
import genealog.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import scala.Tuple7;
import streamingRetention.usecases.carLocal.Tuple2GL;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public class StatsMotionLinearRegression
        implements AggregateFunction<MhealthInputTupleGL, StatsMotionLinearRegression.StatsAverageAccumulator,
        Tuple7GL<Double, Double, Double, Double, Double, Double, String>> {

    private final Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier;

    public StatsMotionLinearRegression(
            Supplier<ProvenanceAggregateStrategy> aggregateStrategySupplier) {
        this.aggregateStrategySupplier = aggregateStrategySupplier;
    }

    @Override
    public StatsAverageAccumulator createAccumulator() {
        return new StatsAverageAccumulator(aggregateStrategySupplier);
    }

    @Override
    public StatsAverageAccumulator add(MhealthInputTupleGL value, StatsAverageAccumulator accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public Tuple7GL<Double, Double, Double, Double, Double, Double, String> getResult(StatsAverageAccumulator accumulator) {
        return accumulator.getAggregatedResult();
    }

    @Override
    public StatsAverageAccumulator merge(StatsAverageAccumulator a, StatsAverageAccumulator b) {
        throw new UnsupportedOperationException("Merge not implemented for provenance windows!");
    }

    public static class StatsAverageAccumulator
            extends GenealogAccumulator<MhealthInputTupleGL,
            Tuple7GL<Double, Double, Double, Double, Double, Double, String>, StatsAverageAccumulator> {

        private final transient ArrayList<Double> xAcceleration = new ArrayList<>();
        private final transient ArrayList<Double> yAcceleration = new ArrayList<>();
        private final transient ArrayList<Double> zAcceleration = new ArrayList<>();
        private final transient ArrayList<Long> times = new ArrayList<>();
        private long timestamp = -1;
        private long stimulus;
        private String key;

        public StatsAverageAccumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
            super(strategySupplier);
        }

        @Override
        public void doAdd(MhealthInputTupleGL tuple) {
            timestamp = Math.max(timestamp, tuple.getTimestamp());
            stimulus = Math.max(stimulus, tuple.getStimulus());
            key = tuple.getKey();
            xAcceleration.add(tuple.getAccelerationChestX());
            yAcceleration.add(tuple.getAccelerationChestY());
            zAcceleration.add(tuple.getAccelerationChestZ());
            times.add(tuple.getTimestamp());
        }

        @Override
        public Tuple7GL<Double, Double, Double, Double, Double, Double, String> doGetAggregatedResult() {
            double meanTimes = calculateLongsMean(times);
            double meanX = calculateDoublesMean(xAcceleration);
            double meanY = calculateDoublesMean(yAcceleration);
            double meanZ = calculateDoublesMean(zAcceleration);

            double slopeX = calculateSlope(times, xAcceleration, meanTimes, meanX);
            double slopeY = calculateSlope(times, yAcceleration, meanTimes, meanY);
            double slopeZ = calculateSlope(times, zAcceleration, meanTimes, meanZ);

            double interceptX = calculateIntercept(meanTimes, meanX, slopeX);
            double interceptY = calculateIntercept(meanTimes, meanY, slopeY);
            double interceptZ = calculateIntercept(meanTimes, meanZ, slopeZ);

            Tuple7GL<Double, Double, Double, Double, Double, Double, String> output = new Tuple7GL<>(
                    meanX, interceptX, meanY, interceptY, meanZ, interceptZ, key);
            output.setStimulus(stimulus);
            output.setTimestamp(timestamp);
            return output;
        }

        @Override
        protected void doMerge(StatsAverageAccumulator other) {
            throw new UnsupportedOperationException("merge");
        }

        protected double calculateDoublesMean(ArrayList<Double> values) {
            double sum = 0;
            for (Double value : values) {
                sum += value;
            }
            return sum / values.size();
        }

        protected double calculateLongsMean(ArrayList<Long> values) {
            double sum = 0;
            for (long value : values) {
                sum += value;
            }
            return sum / values.size();
        }

        protected double calculateSlope(ArrayList<Long> x, ArrayList<Double> y, double meanX, double meanY) {
            double numerator = 0;
            double denominator = 0;

            for (int i = 0; i < x.size(); i++) {
                numerator += (x.get(i) - meanX) * (y.get(i) - meanY);
                denominator += (x.get(i) - meanX) * (x.get(i) - meanX);
            }

            return numerator / denominator;
        }

        protected double calculateIntercept(double meanX, double meanY, double slope) {
            return meanY - slope * meanX;
        }
    }
}
