package com.ibm.enpo.processor.utils;

import com.github.junrar.Archive;
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
        return Mono.fromCallable(() -> {
            Archive archive = new Archive(inputStream, password);
            FileHeader fileHeader;
            while ((fileHeader = archive.nextFileHeader()) != null) {
                if (fileHeader.getFileName().equalsIgnoreCase(name)) {
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    archive.extractFile(fileHeader, outputStream);
                    return outputStream.toByteArray();
                }
            }
            throw new RuntimeException(String.format("Couldn't find the specified file {%s}", name));
        });
    }


}
