package org.zhangyc.test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkademoApplication {
	public static void main(String[] args) {
		SpringApplication.run(KafkademoApplication.class, args);
	}
}