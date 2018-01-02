package com.zhangbaowei.kafkaoffsetbytimestamp.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.StreamSupport;

/***
 * @author zhangbaowei
 *
 */
public class OffsetFinder {
    private static final Logger logger = LoggerFactory.getLogger(OffsetFinder.class);
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

        int retrycount = 0;
        while (assignments.size() == 0 && retrycount++ < 10) {

            consumer.poll(100);
            assignments = consumer.assignment();
            logger.debug("assignments Try " + retrycount);
        }

        if (assignments.size() == 0)
            throw new RuntimeException("assignments is 0!");

        consumer.pause(assignments);

        logger.debug("----------------------------------------------------------------------");

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
                logger.debug("*****************************************************************************");
                logger.error("ERRPOR IN " + topicPartition, e);
                e.printStackTrace();
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


        logger.debug("middle " + begin + "-" + end);

        if (begin.getOffset() > end.getOffset()) {
            throw new RuntimeException("end > begin");
        }

        long offset = (begin.getOffset() + end.getOffset()) / 2L;
        consumer.seek(begin.getTopicPartition(), offset);

        ConsumerRecords<String, String> records = OffsetUtils.RetryGetConsumerRecords(consumer, begin.getTopicPartition());

        if (records.count() == 0) {
            throw new RuntimeException(begin.getTopicPartition() + " FindMiddle Record is 0!");
        }


        Optional<ConsumerRecord<String, String>> max = StreamSupport.stream(records.spliterator(), false).max(Comparator.comparingLong(ConsumerRecord::timestamp));
        Optional<ConsumerRecord<String, String>> min = StreamSupport.stream(records.spliterator(), false).min(Comparator.comparingLong(ConsumerRecord::timestamp));

        if (min.isPresent() && max.isPresent()
                && (min.get().offset() == max.get().offset())
                && (min.get().offset() + 1 == end.getOffset())) {
            //只有一个元素 poll出来,被逼近到左右一个元素的情况下
            logger.debug("records = 1 ，[NOTICE IT]");
            //records.forEach(x -> System.out.println(x));

            return new OffsetTimeStamp(begin.getTopicPartition(), min.get().offset(), min.get().timestamp(), true);
        }

        System.out.println("max=\t" + max.get().timestamp() + "\r\nmin=\t" + min.get().timestamp() + "\r\ntaget=\t" + timestamp);

        //在中间这个范围
        if (max.isPresent() && min.isPresent() && min.get().timestamp() <= timestamp && max.get().timestamp() >= timestamp) {

            Optional<ConsumerRecord<String, String>> first = StreamSupport.stream(records.spliterator(), false)
                    .filter(x -> x.timestamp() >= timestamp)
                    .sorted(Comparator.comparingLong(ConsumerRecord::timestamp))
                    .findFirst();

//            StreamSupport.stream(records.spliterator(), false).forEach(x -> {
//                System.out.println((x.timestamp() - timestamp) + "==" + formatConsumerRecord(x));
//            });
            return getHitOffsetTimeStamp(begin.getTopicPartition(), first);
        }

        if (max.isPresent() && max.get().timestamp() < timestamp) {
            return FindMiddle(new OffsetTimeStamp(begin.getTopicPartition(), max.get().offset(), max.get().timestamp()), end);
        }

        if (min.isPresent() && min.get().timestamp() > timestamp) {
            return FindMiddle(begin, new OffsetTimeStamp(begin.getTopicPartition(), min.get().offset(), min.get().timestamp()));
        }

        throw new RuntimeException("Not Fount ");
    }

    private String formatConsumerRecord(ConsumerRecord<String, String> x) {
        return String.format("offset=%s , timestamp=%s ", x.offset(), x.timestamp());
    }

    private OffsetTimeStamp SeekRangeInitBegin(TopicPartition topicPartition) {


        consumer.seekToBeginning(Arrays.asList(topicPartition));
        ConsumerRecords<String, String> records = OffsetUtils.RetryGetConsumerRecords(consumer, topicPartition);

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


    private OffsetTimeStamp SeekRangeInitEnd(TopicPartition topicPartition) {

        consumer.seekToEnd(Arrays.asList(topicPartition));

        long position = consumer.position(topicPartition);

        consumer.seek(topicPartition, position - 1);

        ConsumerRecords<String, String> records = OffsetUtils.RetryGetConsumerRecords(consumer, topicPartition);

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
