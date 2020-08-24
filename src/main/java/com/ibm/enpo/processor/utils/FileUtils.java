package com.ibm.enpo.processor.utils;

import reactor.core.publisher.Flux;

import java.io.*;

/**
 * @author bishoybasily
 * @since 2020-08-24
 */
public class FileUtils {

    public static Flux<String> lines(byte[] bytes) {
        return Flux.create(sink -> {

            InputStream inputStream = null;
            BufferedReader bufferedReader;

            try {

                inputStream = new ByteArrayInputStream(bytes);
                bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                String line;

                while ((line = bufferedReader.readLine()) != null)
                    sink.next(line);

            } catch (IOException e) {
                sink.error(e);
            } finally {
                try {
                    if (inputStream != null)
                        inputStream.close();
                } catch (Exception ex) {
                    sink.error(ex);
                }
            }

        });
    }

}
