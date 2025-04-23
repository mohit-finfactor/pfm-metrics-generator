package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@EnableJpaRepositories
@SpringBootApplication
@EntityScan({"com.ftpl.pfm.common.model"})
public class Main {
    public static void main(final String[] args) {
        SpringApplication.run(Main.class, args);
    }

}
