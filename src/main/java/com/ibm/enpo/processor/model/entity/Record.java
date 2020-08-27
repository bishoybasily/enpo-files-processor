package com.ibm.enpo.processor.model.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * @author bishoybasily
 * @since 2020-08-24
 */
@Slf4j
@Data
@Accessors(chain = true)
public class Record {

    private String id, name, reference, date, value;

    public static Mono<Record> from(String line) {
        return Mono.fromCallable(() -> {
            String a = line.substring(0, 20).trim();
            String b = line.substring(20, 44).trim();
            String c = line.substring(44, 69).trim();
            String d = line.substring(69, 78).trim();
            return new Record().setId(a).setName(b).setReference(c).setDate(d);
        }).onErrorResume(it -> {
            log.warn("Error while trying to parse ({}) as entry, line is skipped", line);
            return Mono.empty();
        });
    }

    public String[] toStringArray() {
        return new String[]{id, name, reference, date, value};
    }

}
