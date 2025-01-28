package com.example.superhero_localstack;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SuperheroLocalstackApplication {

	public static void main(String[] args) {
		SpringApplication.run(SuperheroLocalstackApplication.class, args);
	}

}
