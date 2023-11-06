package my.flink.project;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction.ReadOnlyContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.runtime.JobException;



import org.apache.flink.core.fs.Path;

public class Bank {

    public static final MapStateDescriptor<String, AlarmedCustomer> alarmedCustStateDescriptor =
    new MapStateDescriptor<String, AlarmedCustomer>("alarmed_customers", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(AlarmedCustomer.class));    

    public void checkAlarmedCustomers(){

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "172.16.67.140:9092");  // adjust with your Kafka brokers
        kafkaProps.setProperty("group.id", "flink-kafka-example");
        kafkaProps.setProperty("auto.offset.reset", "earliest");
            
        String filePath = "alarmed_cust.txt";
        final FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineFormat(),  new Path(filePath))
                                             .build();
        final DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        DataStream<AlarmedCustomer> alarmedCustomers = stream
            .map(new MapFunction<String, AlarmedCustomer>() {
                public AlarmedCustomer map(String value)
                {
                 return new AlarmedCustomer(value);
                }
        });

        BroadcastStream<AlarmedCustomer> alarmedCustBroadcast =	 alarmedCustomers.broadcast(alarmedCustStateDescriptor);

        // (*) Recive transaction data from socket datasource, keyed by customer_id     	
	// 	DataStream<Tuple2<String, String>> data = env.socketTextStream("localhost", 9999)
    //                .map(new MapFunction<String, Tuple2<String, String>>()
    //     {
    //         public Tuple2<String, String> map(String value)
    //         {
    //             String[] words = value.split(",");            
    //             return new Tuple2<String, String>(words[3], value); //{(id_347hfx) (HFXR347924,2018-06-14 23:32:23,Chandigarh,id_347hfx,hf98678167,123302773033,774
    //         }
    //     });

        // Configure Kafka producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    
        // (*) Recive transaction data from kafka datasource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                                            .setBootstrapServers("172.16.67.140:9092")
                                            .setGroupId("flink-kafka-example")
                                            .setTopics("flink-kafka-topic")
                                            .setStartingOffsets(OffsetsInitializer.earliest()) 
                                            .setValueOnlyDeserializer(new SimpleStringDeserializer())            
                                            .build();

        //deprecated datasource
        // DataStream<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<>(
        //         "flink-kafka-topic",          // replace with your topic
        //         new SimpleStringSchema(),
        //         kafkaProps
        //     ));
        
        DataStream<Tuple2<String, String>> kafkaStream = env.fromSource(
            kafkaSource,
             WatermarkStrategy.noWatermarks(),
             "KafkaSource")
             .map(new MapFunction<String, Tuple2<String, String>>()
            {
                public Tuple2<String, String> map(String value)
                {
                    String[] words = value.split(",");            
                    return new Tuple2<String, String>(words[3], value); 
                }
            });

        // kafkaStream.print();

        // (1) Check against alarmed customers
        DataStream<Tuple2<String, String>> alarmedCustTransactions = kafkaStream
        .keyBy(value -> value.f0)
        // .keyBy(0)
        .connect(alarmedCustBroadcast)
        .process(new AlarmedCustCheck());

        DataStream<Tuple2<String, String>> AllFlaggedTxn =	   
        alarmedCustTransactions	;//.union(alarmedCustTransactions);
                                                        
        alarmedCustTransactions.print();

        //  FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(
        //     "my-topic",
        //     new SimpleStringSchema(),
        //     properties
        // );

        // Define a SerializationSchema for Tuple2
        SerializationSchema<Tuple2<String, String>> tuple2SerializationSchema = new SerializationSchema<Tuple2<String, String>>() {
            @Override
            public byte[] serialize(Tuple2<String, String> element) {
                // Convert Tuple2 to a String, for example by concatenating the elements with a comma
                return (element.f0 + "," + element.f1).getBytes();
            }
        };

        // DataStream<String> processedStream = kafkaStream.map(value -> "Processed: " + value);
        // processedStream.print();
        
        // Create Kafka Sink
        KafkaSink<Tuple2<String, String>> sink = KafkaSink.<Tuple2<String, String>>builder()
        // KafkaSink<String> sink = KafkaSink.<String>builder()
                        .setBootstrapServers("172.16.67.140:9092")
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("output-topic")
                            // .setKeySerializationSchema(new SimpleStringSchema())                            
                            .setValueSerializationSchema(new SerializationSchema<Tuple2<String, String>>() {
                                @Override
                                public byte[] serialize(Tuple2<String, String> element) {
                                    return (element.f0 + "," + element.f1).getBytes();
                                }
                            })
                            // .setValueSerializationSchema(tuple2SerializationSchema)
                            .build())
                        .build();

       alarmedCustTransactions.sinkTo(sink);

        //  List<String> myList = Arrays.asList("apple", "banana", "cherry");
        // List<String> upperCaseList = myList.stream()
        //                            .map(String::toUpperCase)
        //                            .collect(Collectors.toList());


        try {
            env.execute("Alarmed Customers job");
        } catch (Exception e) {            
            e.printStackTrace();
        }
    }

    public class AlarmedCustCheck extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, AlarmedCustomer, 
    Tuple2<String, String>>
    {

        public void processElement(Tuple2<String, String> value,ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception
        {
            for (Map.Entry<String, AlarmedCustomer> custEntry: ctx.getBroadcastState(alarmedCustStateDescriptor).immutableEntries())
            {
                final String alarmedCustId = custEntry.getKey();
                final AlarmedCustomer cust = custEntry.getValue();

                // get customer_id of current transaction
                final String tId = value.f1.split(",")[3];
                if (tId.equals(alarmedCustId))
                {
                    out.collect(new Tuple2<String, String>("____ALARM___","Transaction: " + value + " is by an ALARMED customer"));
                }
            }	
        }

        public void processBroadcastElement(AlarmedCustomer cust, Context ctx,Collector<Tuple2<String, String>> out)throws Exception
        {
            ctx.getBroadcastState(alarmedCustStateDescriptor).put(cust.id, cust);
        }    
        
    }

     // SimpleStringDeserializer for demonstration purposes
     public static class SimpleStringDeserializer implements DeserializationSchema<String> {

        @Override
        public String deserialize(byte[] message) {
            return new String(message);
        }

        @Override
        public boolean isEndOfStream(String nextElement) {
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

}

