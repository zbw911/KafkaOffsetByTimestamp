package com.zhangbaowei.kafkaoffsetbytimestamp.utils;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetUtils {
    private static final Logger logger = LoggerFactory.getLogger(OffsetUtils.class);

    public static ConsumerRecords<String, String> RetryGetConsumerRecords(KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
        ConsumerRecords<String, String> records = null;
        //对于多次的调用
        for (int i = 0; i < 10; i++) {
            records = consumer.poll(100);

            int count = records.count();

            if (count == 0) {
                logger.debug(topicPartition.toString() + " currentPosin = " + consumer.position(topicPartition) + " is NULL => TRY " + i);
                continue;
            } else {
                break;
            }
        }
        if (records == null)
            logger.debug("Try count Out " + 10);
        return records;
    }

}
