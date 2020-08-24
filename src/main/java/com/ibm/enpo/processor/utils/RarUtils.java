package com.ibm.enpo.processor.utils;

import com.github.junrar.Archive;
import com.github.junrar.exception.RarException;
import com.github.junrar.rarfile.FileHeader;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

/**
 * @author bishoybasily
 * @since 2020-08-24
 */
public class RarUtils {

    public static Mono<byte[]> extract(String name, InputStream inputStream, String password) {
        return Mono.fromCallable(() -> extract(name, new Archive(inputStream, password)));
    }

    public static Mono<byte[]> extract(String name, InputStream inputStream) {
        return Mono.fromCallable(() -> extract(name, new Archive(inputStream)));
    }

    private static byte[] extract(String name, Archive archive) throws RarException {
        FileHeader fileHeader;
        while ((fileHeader = archive.nextFileHeader()) != null) {
            String fileName = fileHeader.getFileName();
            if (fileName.equalsIgnoreCase(name)) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                archive.extractFile(fileHeader, outputStream);
                return outputStream.toByteArray();
            }
        }
        throw new RarException(new Throwable(String.format("Couldn't find the specified file {%s}", name)));
    }

}
