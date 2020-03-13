package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class StreamJobUtils {

    private StreamJobUtils() {
    }

    public static DataStream<Tuple2<Long, Integer>> parseData(DataStream<String> source) {
        return source
                /* Parse the string data */
                .map(new mapData())
                /* Flink needs to know the events’ timestamps */
                .assignTimestampsAndWatermarks(new timeStampExtractor());
    }

    private static class mapData implements MapFunction<String, Tuple2<Long, Integer>> {
        @Override
        public Tuple2<Long, Integer> map(String string) throws Exception {
            /* Split the data */
            String[] data = string.split(",");
            /* Save Time and Spd */
            return new Tuple2<Long, Integer>(
                    Long.parseLong(data[1]), Integer.parseInt(data[3]));
        }
    }

    private static class timeStampExtractor extends AscendingTimestampExtractor<Tuple2<Long, Integer>> {
        @Override
        public long extractAscendingTimestamp(
                Tuple2<Long, Integer> values) {
            /* Flink wants millisec */
            return values.f0 * 1000;
        }
    }

    public static DataStream<Tuple2<Long, Integer>> streamJobBuilder(DataStream<Tuple2<Long, Integer>> dataStream, boolean gt) {
        return dataStream
                /* Filter data */
                .filter(new SpeedFilter(gt))
                /* Compute over a tumbling window of 1 minute */
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(60)))
                /* Calc average speed */
                .apply(new AverageSpeedWindow());
    }

    private static class SpeedFilter implements FilterFunction<Tuple2<Long, Integer>> {
        /* Greater than */
        private boolean gt;

        public SpeedFilter(boolean state) {
            this.gt = state;
        }

        @Override
        public boolean filter(Tuple2<Long, Integer> values) throws Exception {
            int speed = values.f1;
            return gt ? speed > 20 : speed < 20;
        }
    }

    private static class AverageSpeedWindow implements AllWindowFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>, TimeWindow> {
        @Override
        public void apply(TimeWindow window,
                          Iterable<Tuple2<Long, Integer>> values,
                          Collector<Tuple2<Long, Integer>> collector) throws Exception {
            Integer sum = 0;
            int count = 0;
            for (Tuple2<Long, Integer> tuple : values) {
                sum += tuple.f1;
                count++;
            }
            collector.collect(new Tuple2<Long, Integer>(window.maxTimestamp(), sum / count));
        }
    }

    public static DataStream<Object> joinStreams(DataStream<Tuple2<Long, Integer>> avgLow, DataStream<Tuple2<Long, Integer>> avgHigh) {
        /* Join average streams */
        return avgLow.join(avgHigh)
                .where(new TimeWindowKeySelector())
                .equalTo(new TimeWindowKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .apply(new JoinAverage());
    }

    private static class JoinAverage implements JoinFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Object> {
        @Override
        public Tuple4<Long, Integer, Integer, String> join(
                Tuple2<Long, Integer> first,
                Tuple2<Long, Integer> second) throws Exception {
            /* |A2-A1| */
            int abs = Math.abs(first.f1 - second.f1);

            Tuple4 res = new Tuple4(first.f0, first.f1, second.f1, null);
            res.setField(abs > 20 ? "alert" : "ok", 3);

            return res;
        }
    }

    private static class TimeWindowKeySelector implements KeySelector<Tuple2<Long, Integer>, Long> {
        @Override
        public Long getKey(Tuple2<Long, Integer> values) throws Exception {
            return values.f0;
        }
    }

}
