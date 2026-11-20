package net.lzip.ant.handler;

import com.xxl.job.core.handler.annotation.XxlJob;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class PrintTime {

    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @XxlJob("printTime")
    public void printTime() {
        String format = dateTimeFormatter.format(LocalDateTime.now());
        System.out.println(format);
    }
}
