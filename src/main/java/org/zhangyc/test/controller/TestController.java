package org.zhangyc.test.controller;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.*;
import org.zhangyc.test.listener.Listener;

import java.util.concurrent.TimeUnit;

/**
 * Created by gongye1 on 2017/3/13.
 */
@RestController
public class TestController {
    private static final Logger logger = Logger.getLogger(Listener.class);
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping(value="/sendMsg/{message}", method= RequestMethod.GET)
    public String sendMsg(@PathVariable String message){
        logger.info("message:"+ message);
        for(int i = 0; i < 10000; i++) {
            ListenableFuture future = kafkaTemplate.send("ibdtest", message);
            logger.info("future:" + JSON.toJSONString(future));
            try{
                TimeUnit.MILLISECONDS.sleep(100);
            }catch (Throwable throwable){
                throwable.printStackTrace();
            }
        }
        return "{\"code\":200}";
    }
}
