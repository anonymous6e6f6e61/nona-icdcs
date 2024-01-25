package streamingRetention;

import genealog.GenealogData;
import genealog.GenealogDataSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streamingRetention.usecases.CountTupleGL;
import streamingRetention.usecases.carLocal.*;
import streamingRetention.usecases.linearRoad.LinearRoadInputTupleGL;
import streamingRetention.usecases.linearRoad.TollTupleGL;
import streamingRetention.usecases.riot.MhealthInputTupleGL;
import streamingRetention.usecases.synthetic.SyntheticSinkTuple;
import streamingRetention.usecases.synthetic.SyntheticSourceTuple;
import util.ExperimentSettings;
import util.TimestampedUIDTuple;

public enum NonaFlinkSerializerActivator {
    LINEAR_ROAD_FULL {
        @Override
        public void activate(StreamExecutionEnvironment env, ExperimentSettings settings) {
            env.registerType(TimestampedUIDTuple[].class);
            env.registerType(WTimestampedUIDTuple.class);
            env.registerType(CountTupleGL.class);
            env.registerType(LinearRoadInputTupleGL.class);
            env.registerType(TollTupleGL.class);
            env.addDefaultKryoSerializer(
                    GenealogData.class,
                    GenealogDataSerializer.newInstance(
                            settings.aggregateStrategySupplier().get(),
                            settings.statisticsFolder(),
                            settings.graphTraversalStatistics())
            );
        }
    },
    CAR_LOCAL_FULL {
        @Override
        public void activate(StreamExecutionEnvironment env, ExperimentSettings settings) {
            env.registerType(TimestampedUIDTuple[].class);
            env.registerType(WTimestampedUIDTuple.class);
            env.registerType(CarLocalInputTupleGL.class);
            env.registerType(CrossedPedestriansTuple.class);
            env.registerType(CyclesInFrontTuple.class);
            env.addDefaultKryoSerializer(
                    GenealogData.class,
                    GenealogDataSerializer.newInstance(
                            settings.aggregateStrategySupplier().get(),
                            settings.statisticsFolder(),
                            settings.graphTraversalStatistics())
            );
        }
    },
    RIOT_MHEALTH_FULL {
        @Override
        public void activate(StreamExecutionEnvironment env, ExperimentSettings settings) {
            env.registerType(TimestampedUIDTuple[].class);
            env.registerType(WTimestampedUIDTuple.class);
            env.registerType(MhealthInputTupleGL.class);
            env.addDefaultKryoSerializer(
                    GenealogData.class,
                    GenealogDataSerializer.newInstance(
                            settings.aggregateStrategySupplier().get(),
                            settings.statisticsFolder(),
                            settings.graphTraversalStatistics())
            );
        }
    },
    SYNTHETIC {
        public void activate(StreamExecutionEnvironment env, ExperimentSettings settings) {
            env.registerType(TimestampedUIDTuple[].class);
            env.registerType(WTimestampedUIDTuple.class);
            env.registerType(SyntheticSourceTuple.class);
            env.registerType(SyntheticSinkTuple.class);
            env.addDefaultKryoSerializer(
                    GenealogData.class,
                    GenealogDataSerializer.newInstance(
                            settings.aggregateStrategySupplier().get(),
                            settings.statisticsFolder(),
                            settings.graphTraversalStatistics())
            );
        }
    },
    MINIMUM {
        @Override
        public void activate(StreamExecutionEnvironment env, ExperimentSettings settings) {
            env.addDefaultKryoSerializer(
                    GenealogData.class,
                    GenealogDataSerializer.newInstance(
                            settings.aggregateStrategySupplier().get(),
                            settings.statisticsFolder(),
                            settings.graphTraversalStatistics())
            );
        }
    };

    public abstract void activate(StreamExecutionEnvironment env, ExperimentSettings settings);

}
