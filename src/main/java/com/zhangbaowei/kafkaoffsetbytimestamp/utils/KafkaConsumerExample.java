package com.zhangbaowei.kafkaoffsetbytimestamp.utils;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class KafkaConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerExample.class);
    private static long timestamp = -1;


    public static void main(String[] args) throws IOException {
        if (args != null && args.length > 0) {
            if (args.length > 0) {
                timestamp = Long.parseLong(args[2]);
                logger.info("timestamp=" + timestamp);
            }


        } else {
            logger.info("NO ARGS");
        }

        new KafkaConsumerExample().consume();
    }

    public void consume() {
        PropertiesLoader propertiesLoader = new PropertiesLoader("application.properties");

        String bootstrapServers = propertiesLoader.getProperty("bootstrap.servers");
        ;
        String topicname = propertiesLoader.getProperty("topic");
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        //  props.put("group.id", "test1234");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("group.id", "jd-group1");
        //props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class);
        //  props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", StringDeserializer.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topicname));


        if (timestamp >= 0) {
            OffsetFinder offsetFinder = new OffsetFinder(consumer, timestamp);
            ArrayList<OffsetTimeStamp> get = offsetFinder.Get();


            get.forEach(x -> {
                System.out.println(x);
                consumer.seek(x.getTopicPartition(), x.getOffset());
            });
        }


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);


            int count = records.count();


            if (count == 0) {
                //如果没数据就休息会儿
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (ConsumerRecord<String, String> record : records) {

                Print(records, 0);

            }


        }

    }


    private void Print(ConsumerRecords<String, String> records, int top) {

        int i = 0;
        for (ConsumerRecord<String, String> record : records) {
            if (top > 0 && i++ > top)
                break;
            logger.info(String.format("date= %s  timestamp=%s offset = %d, key = %s, value = %s%n",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()),
                    new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date(record.timestamp())),
                    record.offset(), "key"/*record.key()*/, record.value()));
        }
    }
}

//    private void ResetOffset(KafkaConsumer<String, String> consumer) {
//
//        Set<TopicPartition> assignments = consumer.assignment();
//        Map<TopicPartition, Long> query = new HashMap<>();
//        for (TopicPartition topicPartition : assignments) {
//            query.put(
//                    topicPartition,
//                    Instant.now().minus(10, SECONDS).toEpochMilli());
//
//        }
//
//
//        for (TopicPartition topicPartition : assignments) {
//            consumer.seekToBeginning(Arrays.asList(topicPartition));
//            // long position = consumer.position(topicPartition);
//
////            ConsumerRecords<String, String> records = consumer.poll(100);
////
////            for (ConsumerRecord<String, String> record : records) {
////
////                long timestamp = record.timestamp();
////                Calendar calendar = Calendar.getInstance();
////                calendar.setTimeInMillis(timestamp);
////                long position = consumer.position(topicPartition);
////
////                System.out.println(record.offset() + "=>" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime()));
////            }
//        }


//    }
