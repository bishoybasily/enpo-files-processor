package com.ibm.enpo.processor.utils;

import org.springframework.core.io.buffer.DataBuffer;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.function.BiFunction;

/**
 * @author bishoybasily
 * @since 2020-08-16
 */
public class IOUtils {

    public static class Input {

        public static InputStream empty() {
            return new InputStream() {
                public int read() {
                    return -1;
                }
            };
        }

        public static BiFunction<InputStream, DataBuffer, InputStream> dataBufferInputStreamAccumulator() {
            return (inputStream, dataBuffer) -> new SequenceInputStream(inputStream, dataBuffer.asInputStream());
        }

    }

}
