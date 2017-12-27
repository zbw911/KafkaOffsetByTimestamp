package com.zhangbaowei.kafkaoffsetbytimestamp.utils;

import org.apache.kafka.common.TopicPartition;

public class OffsetTimeStamp {
    private TopicPartition topicPartition;
    private long offset;
    private long timestamp;
    private boolean hit;

    public OffsetTimeStamp() {

    }

    public OffsetTimeStamp(TopicPartition topicPartition, long offset, long timestamp) {
        this.topicPartition = topicPartition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public OffsetTimeStamp(TopicPartition topicPartition, long offset, long timestamp, boolean hit) {
        this.topicPartition = topicPartition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.hit = hit;
    }

    public boolean isHit() {
        return hit;
    }

    public void setHit(boolean hit) {
        this.hit = hit;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public void setTopicPartition(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {

        return String.format("hit= %s, topicPartition=%s, offset=%s ,timestamp=%s ,date=%s", hit, topicPartition, offset, timestamp, new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(timestamp)));
    }
}
