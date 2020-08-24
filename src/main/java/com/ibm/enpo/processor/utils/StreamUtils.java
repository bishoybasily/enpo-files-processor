package com.ibm.enpo.processor.utils;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author bishoybasily
 * @since 2020-08-24
 */
public class StreamUtils {

    public static <T> Stream<T> sequential(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public static <T> Stream<T> parallel(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), true);
    }

}
