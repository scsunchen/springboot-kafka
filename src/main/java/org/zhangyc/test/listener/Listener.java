package org.zhangyc.test.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import java.util.Optional;

/**
 * Created by gongye1 on 2017/3/13.
 */
public class Listener {
    private static final Logger logger = Logger.getLogger(Listener.class);
    /**
     * 监听kafka消息,如果有消息则消费,同步数据到新烽火的库
     * @param record 消息实体bean
     */
    @KafkaListener(topics = "ibdtest", group = "test-consumer-group")
    public void listen(ConsumerRecord<?, ?> record) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            try {
                Object message = kafkaMessage.get();
               logger.info("listen: "+message.toString());
            } catch (Throwable throwable) {
                logger.error(throwable.getMessage(), throwable);
            }
        }
    }
}
