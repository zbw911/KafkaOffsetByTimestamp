package com.zhangbaowei.kafkaoffsetbytimestamp.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/***
 * @author zhangbaowei
 *
 */
public class OffsetFinder {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerExample.class);
    private KafkaConsumer<String, String> consumer;

    private long timestamp;

    private ArrayList<OffsetTimeStamp> list = new ArrayList<>();

    public OffsetFinder(KafkaConsumer<String, String> consumer, long timestamp) {
        this.consumer = consumer;
        this.timestamp = timestamp;
    }


    public ArrayList<OffsetTimeStamp> Get() {
        S();
        return list;
    }


    private void S() {
        consumer.poll(0);
        Set<TopicPartition> assignments = consumer.assignment();

        consumer.pause(assignments);
//        consumer.paused().forEach(x -> System.out.println(x.toString()));

        logger.info("----------------------------------------------------------------------");

        for (TopicPartition topicPartition : assignments) {

            consumer.resume(Arrays.asList(topicPartition));

            try {
                OffsetTimeStamp offsetTimeStamp = SeekRangeInitBegin(topicPartition);

                logger.debug(offsetTimeStamp.toString());

                if (offsetTimeStamp.isHit()) {
                    list.add(offsetTimeStamp);
                    continue;
                }

                OffsetTimeStamp offsetTimeStamp1 = SeekRangeInitEnd(topicPartition);

                logger.debug(offsetTimeStamp1.toString());

                if (offsetTimeStamp1.isHit()) {
                    list.add(offsetTimeStamp1);
                    continue;
                }
                OffsetTimeStamp offsetTimeStamp2 = FindMiddle(offsetTimeStamp, offsetTimeStamp1);

                logger.debug(offsetTimeStamp2.toString());

                if (offsetTimeStamp2.isHit()) {
                    list.add(offsetTimeStamp2);
                    continue;
                }
            } catch (Exception e) {

                e.printStackTrace();
                logger.error("error", e);
            } finally {
                consumer.pause(Arrays.asList(topicPartition));
            }
        }


        consumer.resume(assignments);

    }

    /**
     * 查找中间部分
     *
     * @param begin
     * @param end
     * @return
     */
    private OffsetTimeStamp FindMiddle(OffsetTimeStamp begin, OffsetTimeStamp end) {


        logger.info("middle " + begin + "-" + end);

        if (begin.getOffset() > end.getOffset()) {
            throw new RuntimeException("end > begin");
        }

        long offset = (begin.getOffset() + end.getOffset()) / 2L;
        consumer.seek(begin.getTopicPartition(), offset);

        ConsumerRecords<String, String> records = RetryGetConsumerRecords(begin.getTopicPartition());

        if (records.count() == 0) {
            throw new RuntimeException(begin.getTopicPartition() + " FindMiddle Record is 0!");
        }

        Optional<ConsumerRecord<String, String>> first = StreamSupport.stream(records.spliterator(), false).filter(x -> x.timestamp() >= timestamp).sorted(Comparator.comparingLong(ConsumerRecord::timestamp)).findFirst();

        Optional<ConsumerRecord<String, String>> max = StreamSupport.stream(records.spliterator(), false).max(Comparator.comparingLong(ConsumerRecord::timestamp));
        Optional<ConsumerRecord<String, String>> min = StreamSupport.stream(records.spliterator(), false).min(Comparator.comparingLong(ConsumerRecord::timestamp));


        if (first.isPresent()) {
            logger.info("asc begin");
            Stream<ConsumerRecord<String, String>> sorted = StreamSupport.stream(records.spliterator(), false).filter(x -> x.timestamp() >= timestamp).sorted(Comparator.comparingLong(ConsumerRecord::timestamp)).limit(10);

            sorted.forEach(x -> logger.info(formatConsumerRecord(x)));

            logger.info("asc end");
            logger.info("desc start");
            Stream<ConsumerRecord<String, String>> sorted2 = StreamSupport.stream(records.spliterator(), false).filter(x -> x.timestamp() < timestamp).sorted((x, y) -> Long.compare(y.timestamp(), x.timestamp())).limit(10);

            sorted2.forEach(x -> logger.info(formatConsumerRecord(x)));
            logger.info("desc end");
            return getHitOffsetTimeStamp(begin.getTopicPartition(), first);
        }

        if (max.isPresent() && max.get().timestamp() <= timestamp) {
            return FindMiddle(new OffsetTimeStamp(begin.getTopicPartition(), max.get().offset(), max.get().timestamp()), end);
        }

        if (min.isPresent() && min.get().timestamp() >= timestamp) {
            return FindMiddle(begin, new OffsetTimeStamp(begin.getTopicPartition(), min.get().offset(), min.get().timestamp()));
        }

        // return null;
        //返回默认
//        begin.setTimestamp(-3);
//        begin.setHit(true);
//        return begin;

        throw new RuntimeException("Not Fount ");
    }

    private String formatConsumerRecord(ConsumerRecord<String, String> x) {
        return String.format("offset=%s , timestamp=%s ", x.offset(), x.timestamp());
    }

    private OffsetTimeStamp SeekRangeInitBegin(TopicPartition topicPartition) {


        consumer.seekToBeginning(Arrays.asList(topicPartition));
        ConsumerRecords<String, String> records = RetryGetConsumerRecords(topicPartition);

        if (records.count() == 0) {
            throw new RuntimeException(topicPartition + " SeekRangeInitBegin Record is 0!");
        }

        Optional<ConsumerRecord<String, String>> first = StreamSupport.stream(records.spliterator(), false).filter(x -> x.timestamp() >= timestamp && x.timestamp() <= timestamp).findFirst();

        Optional<ConsumerRecord<String, String>> max = StreamSupport.stream(records.spliterator(), false).max(Comparator.comparingLong(ConsumerRecord::timestamp));
        Optional<ConsumerRecord<String, String>> min = StreamSupport.stream(records.spliterator(), false).min(Comparator.comparingLong(ConsumerRecord::timestamp));

        if (first.isPresent()) {
            return getHitOffsetTimeStamp(topicPartition, first);
        } else if (min.isPresent() && min.get().timestamp() > timestamp) {
            return getHitOffsetTimeStamp(topicPartition, min);
        } else {
            ConsumerRecord<String, String> stringStringConsumerRecord = StreamSupport.stream(records.spliterator(), false).skip(records.count() - 1).findFirst().get();

            OffsetTimeStamp end = new OffsetTimeStamp();
            end.setOffset(stringStringConsumerRecord.offset());
            end.setTimestamp(stringStringConsumerRecord.timestamp());
            end.setTopicPartition(topicPartition);
            return end;
        }


    }

    private ConsumerRecords<String, String> RetryGetConsumerRecords(TopicPartition topicPartition) {
        ConsumerRecords<String, String> records = null;
        //对于多次的调用
        for (int i = 0; i < 10; i++) {
            records = consumer.poll(100);

            int count = records.count();

            if (count == 0) {

                logger.info(topicPartition.toString() + " currentPosin = " + consumer.position(topicPartition) + " is NULL => TRY " + i);
                continue;
            } else {
                break;
            }
        }
        return records;
    }


    private OffsetTimeStamp SeekRangeInitEnd(TopicPartition topicPartition) {

        consumer.seekToEnd(Arrays.asList(topicPartition));

        long position = consumer.position(topicPartition);

        consumer.seek(topicPartition, position - 1);

        ConsumerRecords<String, String> records = RetryGetConsumerRecords(topicPartition);

        if (records.count() == 0) {
            throw new RuntimeException(topicPartition + " SeekRangeInitEnd Record is 0!");
        }

        Optional<ConsumerRecord<String, String>> first = StreamSupport.stream(records.spliterator(), false).filter(x -> x.timestamp() >= timestamp && x.timestamp() <= timestamp).findFirst();
        Optional<ConsumerRecord<String, String>> max = StreamSupport.stream(records.spliterator(), false).max(Comparator.comparingLong(ConsumerRecord::timestamp));
        Optional<ConsumerRecord<String, String>> min = StreamSupport.stream(records.spliterator(), false).min(Comparator.comparingLong(ConsumerRecord::timestamp));


        if (first.isPresent()) {
            return getHitOffsetTimeStamp(topicPartition, first);

        } else if (max.isPresent() && max.get().timestamp() < timestamp) {
            return getHitOffsetTimeStamp(topicPartition, max);
        } else {
            ConsumerRecord<String, String> stringStringConsumerRecord = StreamSupport.stream(records.spliterator(), false).findFirst().get();

            OffsetTimeStamp end = new OffsetTimeStamp();
            end.setOffset(stringStringConsumerRecord.offset());
            end.setTimestamp(stringStringConsumerRecord.timestamp());
            end.setTopicPartition(topicPartition);
            return end;
        }


    }

    private OffsetTimeStamp getHitOffsetTimeStamp(TopicPartition topicPartition, Optional<ConsumerRecord<String, String>> first) {
        ConsumerRecord<String, String> stringStringConsumerRecord = first.get();

        OffsetTimeStamp offsetTimeStamp = new OffsetTimeStamp();
        offsetTimeStamp.setOffset(stringStringConsumerRecord.offset());
        offsetTimeStamp.setTimestamp(stringStringConsumerRecord.timestamp());
        offsetTimeStamp.setTopicPartition(topicPartition);
        offsetTimeStamp.setHit(true);
        return offsetTimeStamp;
    }

}
