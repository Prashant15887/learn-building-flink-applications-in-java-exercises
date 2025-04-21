package flightimporter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.io.InputStream;
import java.util.Properties;
import java.time.ZonedDateTime;

import models.SkyOneAirlinesFlightData;
import models.SunsetAirFlightData;
import models.FlightData;

public class FlightImporterJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerConfig = new Properties();
        try (InputStream inStream = FlightImporterJob.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            consumerConfig.load(inStream);
        }

        Properties producerConfig = new Properties();
        try (InputStream outStream = FlightImporterJob.class.getClassLoader().getResourceAsStream("producer.properties")) {
            producerConfig.load(outStream);
        }

        KafkaSource<SkyOneAirlinesFlightData> skyoneSource = KafkaSource.<SkyOneAirlinesFlightData>builder()
                .setProperties(consumerConfig)
                .setTopics("skyone")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SkyOneAirlinesFlightData.class))
                .build();

        KafkaSource<SunsetAirFlightData> sunsetSource = KafkaSource.<SunsetAirFlightData>builder()
                .setProperties(consumerConfig)
                .setTopics("sunset")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SunsetAirFlightData.class))
                .build();

        DataStream<SkyOneAirlinesFlightData> skyoneStream = env.fromSource(skyoneSource, WatermarkStrategy.noWatermarks(), "skyone_source");

        DataStream<SunsetAirFlightData> sunsetStream = env.fromSource(sunsetSource, WatermarkStrategy.noWatermarks(), "sunset_source");

        KafkaRecordSerializationSchema<FlightData> serializer = KafkaRecordSerializationSchema.<FlightData>builder()
                .setTopic("flightdata")
                .setValueSerializationSchema(
                    new JsonSerializationSchema<FlightData>(
                        () -> {
                            return new ObjectMapper()
                                    .registerModule(new JavaTimeModule());
                        }
                    )
                )
                .build();

        KafkaSink<FlightData> flighDataSink = KafkaSink.<FlightData>builder()
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(serializer)
                .build();

        //skyoneStream.print();
        defineWorkflow(skyoneStream, sunsetStream)
                .sinkTo(flighDataSink)
                .name("flightdata_sink");

        //DataStream<FlightData> flightDataStream = defineWorkflow(skyoneStream);
        //flightDataStream.print();

        env.execute("FlightImporter");
    }

    public static class FlightArrivalTimeFilter1 implements FilterFunction<SkyOneAirlinesFlightData> {
        @Override
        public boolean filter(SkyOneAirlinesFlightData input) {
            ZonedDateTime t1 = input.getFlightArrivalTime();
            ZonedDateTime t2 = ZonedDateTime.now();
            return t1.isAfter(t2);
        }
    }

    public static class FlightDataConverter1 implements MapFunction<SkyOneAirlinesFlightData, FlightData> {
        @Override
        public FlightData map(SkyOneAirlinesFlightData input) {
            return input.toFlightData();
        }
    }

    public static class FlightArrivalTimeFilter2 implements FilterFunction<SunsetAirFlightData> {
        @Override
        public boolean filter(SunsetAirFlightData input) {
            ZonedDateTime t1 = input.getArrivalTime();
            ZonedDateTime t2 = ZonedDateTime.now();
            return t1.isAfter(t2);
        }
    }

    public static class FlightDataConverter2 implements MapFunction<SunsetAirFlightData, FlightData> {
        @Override
        public FlightData map(SunsetAirFlightData input) {
            return input.toFlightData();
        }
    }

    public static class FlightArrivalTimeFilter3 implements FilterFunction<FlightData> {
        @Override
        public boolean filter(FlightData input) {
            ZonedDateTime t1 = input.getArrivalTime();
            ZonedDateTime t2 = ZonedDateTime.now();
            return t1.isAfter(t2);
        }
    }

    /*public static DataStream<FlightData> defineWorkflow(DataStream<SkyOneAirlinesFlightData> skyOneSource, DataStream<SunsetAirFlightData> sunsetSource){
        DataStream<FlightData> stream1 = skyOneSource
                .filter(new FlightArrivalTimeFilter1())
                .map(new FlightDataConverter1());
        DataStream<FlightData> stream2 = sunsetSource
                .filter(new FlightArrivalTimeFilter2())
                .map(new FlightDataConverter2());
        return stream1.union(stream2);
    }*/

    public static DataStream<FlightData> defineWorkflow(DataStream<SkyOneAirlinesFlightData> stream1, DataStream<SunsetAirFlightData> stream2){
        ConnectedStreams<SkyOneAirlinesFlightData, SunsetAirFlightData> connected = stream1.connect(stream2);
        DataStream<FlightData> output = connected
                .map(
                    new CoMapFunction<SkyOneAirlinesFlightData, SunsetAirFlightData, FlightData>(){
                        @Override
                        public FlightData map1(SkyOneAirlinesFlightData input){
                            return input.toFlightData();
                        }

                        @Override
                        public FlightData map2(SunsetAirFlightData input){
                            return input.toFlightData();
                        }
                    }
                )
                .filter(new FlightArrivalTimeFilter3());
        return output;
    }
}