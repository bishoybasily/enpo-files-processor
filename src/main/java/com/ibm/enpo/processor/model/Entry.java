package com.ibm.enpo.processor.model;

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
public class Entry {

    private String col1, col2, col3, col4;

    public static Mono<Entry> from(String line) {
        return Mono.fromCallable(() -> {
            String a = line.substring(0, 20).trim();
            String b = line.substring(20, 44).trim();
            String c = line.substring(44, 69).trim();
            String d = line.substring(69, 78).trim();
            return new Entry().setCol1(a).setCol2(b).setCol3(c).setCol4(d);
        }).onErrorResume(it -> {
            log.warn("Error while trying to parse ({}) as entry, line is skipped", line);
            return Mono.empty();
        });
    }

}
