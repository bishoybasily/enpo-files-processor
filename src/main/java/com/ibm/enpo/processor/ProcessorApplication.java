package com.ibm.enpo.processor;

import org.apache.commons.io.IOUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;

@SpringBootApplication
public class ProcessorApplication {


    public static void main(String[] args) {
        SpringApplication.run(ProcessorApplication.class, args);
    }

    @Bean
    public ExecutorService executorService() {
        return Executors.newFixedThreadPool(10);
    }

    @Bean
    public RouterFunction<ServerResponse> processor(ExecutorService executorService) {

        return RouterFunctions.route(
                POST("/processor").and(accept(MediaType.MULTIPART_FORM_DATA)),
                serverRequest -> {

                    Mono<FilePart> filePartMono = serverRequest
                            .body(BodyExtractors.toMultipartData())
                            .map(MultiValueMap::toSingleValueMap)
                            .map(it -> it.get("file"))
                            .map(FilePart.class::cast)
                            .cache();

                    Mono<String> nameMono = filePartMono.map(FilePart::filename);

                    filePartMono.map(FilePart::content)
                            .flatMapMany(XlsxUtils::read)
                            .flatMap(strings -> {
                                return Mono.fromCallable(() -> {
                                    String[] result = Arrays.copyOf(strings, strings.length + 1);
                                    result[strings.length] = UUID.randomUUID().toString();
                                    return result;
                                });
                            })
                            .collectList()
                            .flatMap(XlsxUtils::write)
                            .zipWith(nameMono, (bytes, name) -> {
                                try {
                                    IOUtils.write(bytes, new FileOutputStream(name));
                                    return true;
                                } catch (Exception e) {
                                    return false;
                                }
                            })
                            .subscribe();

                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(nameMono.map(Response::new), Response.class);

                }
        );
    }

}
