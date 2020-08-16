package com.ibm.enpo.processor;

import lombok.Data;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.reactivestreams.Publisher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;

@SpringBootApplication
public class ProcessorApplication {

    private final static DataFormatter FORMATTER = new DataFormatter();

    public static void main(String[] args) {
        SpringApplication.run(ProcessorApplication.class, args);
    }

    @Bean
    public RouterFunction<ServerResponse> processor() {

        return RouterFunctions.route(
                POST("/processor").and(accept(MediaType.MULTIPART_FORM_DATA)),
                serverRequest -> {

                    Publisher<String> response = serverRequest.body(BodyExtractors.toMultipartData())
                            .map(MultiValueMap::toSingleValueMap)
                            .map(it -> it.get("file"))
                            .map(FilePart.class::cast)
                            .map(FilePart::content)
                            .flatMapMany(content -> {

                                content.reduce(empty(), (InputStream s, DataBuffer d) -> new SequenceInputStream(s, d.asInputStream()))
                                        .flatMapIterable(new Function<InputStream, Iterable<Sheet>>() {
                                            @SneakyThrows
                                            @Override
                                            public Iterable<Sheet> apply(InputStream inputStream) {
                                                return new XSSFWorkbook(inputStream);
                                            }
                                        })
                                        .flatMapIterable(rows -> rows)
                                        .skip(0)
                                        .map(row -> {
                                            int length = row.getPhysicalNumberOfCells();
                                            String[] tokens = new String[length];
                                            for (int i = 0; i < length; i++)
                                                tokens[i] = FORMATTER.formatCellValue(row.getCell(i));
                                            return tokens;
                                        })
                                        .subscribe(new Consumer<String[]>() {
                                            @Override
                                            public void accept(String[] strings) {

                                            }
                                        });

                                return content.map(buffer -> {
                                    byte[] bytes = new byte[buffer.readableByteCount()];
                                    buffer.read(bytes);
                                    DataBufferUtils.release(buffer);
                                    return new String(bytes, StandardCharsets.UTF_8);
                                });
                            });

                    return ServerResponse.ok()
                            .contentType(MediaType.TEXT_PLAIN)
                            .body(response, String.class);

                }
        );
    }

    private InputStream empty() {
        return new InputStream() {
            public int read() {
                return -1;
            }
        };
    }

    @Data
    @Accessors(chain = true)
    public static class Body {

        private String id;

    }

}
