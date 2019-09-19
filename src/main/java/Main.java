import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

public class Main
{
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment();
        FlinkKafkaConsumer09<String> flinkKafkaConsumer = createStringConsumerForTopic("topic","localhost:9092");

        flinkKafkaConsumer.setStartFromLatest();

        DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);

        stringInputStream.timeWindowAll(Time.seconds(15), Time.seconds(5)).apply((AllWindowFunction<String, String,
                TimeWindow>) (timeWindow, iterable, collector) -> iterable.forEach(json -> {
                    try
                    {
                        System.out.println(json);
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                }));

        environment.execute();
    }

    public static FlinkKafkaConsumer09<String> createStringConsumerForTopic(
            String topic, String kafkaAddress)
    {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        FlinkKafkaConsumer09<String> consumer = new FlinkKafkaConsumer09<>(
                topic, new SimpleStringSchema(), props);

        return consumer;
    }
}
