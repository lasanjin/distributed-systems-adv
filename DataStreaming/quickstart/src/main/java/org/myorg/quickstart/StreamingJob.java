package org.myorg.quickstart;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamingJob {

    private final static String dataPath = "<path-to-data>";
    private final static String outPath = "<path-to-output>";

    public static void main(String[] args) throws Exception {
        /* Set up the streaming execution environment */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /* To work with event time, streaming programs need to set the time characteristic accordingly */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /* Load data from file */
        DataStream<String> source = env.readTextFile(dataPath);

        /* Get Time and Spd from h1.txt: [[0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos], ...] */
        DataStream<Tuple2<Long, Float>> dataStream = StreamJobUtils.parseData(source);

        /* A1: Average speed (tumbling window of 1 minute) of cars traveling > 20 MPH */
        DataStream<Tuple2<Long, Float>> avgHigh = StreamJobUtils.streamJobBuilder(dataStream, true);

        /* A2: Average speed (tumbling window of 1 minute) of cars traveling < 20 MPH */
        DataStream<Tuple2<Long, Float>> avgLow = StreamJobUtils.streamJobBuilder(dataStream, false);

        /* Join average streams */
        DataStream<Object> joinedStream = StreamJobUtils.joinStreams(avgLow, avgHigh);

        /* Save data to text file or print to log */
        joinedStream.writeAsText(outPath);
        // joinedStream.print();

        env.execute("Average speed");
    }

}
