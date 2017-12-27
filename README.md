# KafkaOffsetByTimestamp

#本项目旨在解决不应该存在的问题

##首先说明一下 在新的版本kafka已经支持根据 timestamp 查找 offset 功能 ， 说见 [这里](https://github.com/jeqo/post-kafka-rewind-consumer-offset/blob/master/src/main/java/io/github/jeqo/posts/kafka/consumer/KafkaConsumerFromTime.java)


## 这个项目仅针对于低版本的 0.1  左右的版本，其它的版本没有试过。


##由于是 spring boot 项目，可以用命令行启动了。
>  java -cp kafkademo-0.0.1-SNAPSHOT.jar -D"loader.main=com.zhangbaowei.kafkaoffsetbytimestamp.KafkaConsumerExample"  -D"loader.args=1514364105000" org.springframework.boot.loader.PropertiesLauncher