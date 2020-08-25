package com.ibm.enpo.processor;

import com.ibm.enpo.processor.config.ProcessorProperties;
import com.ibm.enpo.processor.model.Entry;
import com.ibm.enpo.processor.utils.FileUtils;
import com.ibm.enpo.processor.utils.IOUtils;
import com.ibm.enpo.processor.utils.RarUtils;
import com.ibm.enpo.processor.utils.XlsxUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;

@Slf4j
@ConfigurationPropertiesScan
@SpringBootApplication
public class ProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProcessorApplication.class, args);
    }

    @Bean
    public RouterFunction<ServerResponse> processor(ProcessorProperties props, ExecutorService executor) {

        return RouterFunctions.route(
                POST("/processor").and(accept(MediaType.MULTIPART_FORM_DATA)),
                serverRequest -> {

                    // get the uploaded file
                    Mono<FilePart> filePartMono = serverRequest.body(BodyExtractors.toMultipartData())
                            // extract the uploaded multipart file from the form body
                            .map(map -> (FilePart) map.toSingleValueMap().get("file"))
                            // cache it for multiple consumers
                            .cache();

                    // get file's name
                    Mono<String> nameMono = filePartMono.map(FilePart::filename)
                            // cache it for multiple consumers
                            .cache();

                    // get file's content
                    Flux<Entry> entriesFlux = filePartMono.map(FilePart::content)
                            // reduce the file content input-stream into one single input-stream
                            .flatMap(this::reduce)
                            // extract the compressed file into byte[]
                            .flatMap(inputStream -> unrar(props, inputStream))
                            // read the lines as a stream
                            .flatMapMany(FileUtils::lines)
                            // map every line to an entry object
                            .flatMap(Entry::from)
                            // cache it for multiple consumers
                            .cache();

                    // get the parsed entries count
                    Mono<Integer> totalEntriesMono = entriesFlux.collectList()
                            .map(List::size);

                    // do the processing logic
                    process(executor, nameMono, entriesFlux);

                    /*
                     * for the http part return a response shows that the file has been received and it is being processed.
                     */
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(nameMono.zipWith(totalEntriesMono, Response::new), Response.class);

                }
        );
    }

    private void process(ExecutorService executor, Mono<String> nameMono, Flux<Entry> entriesFlux) {
        // run the next tasks in parallel (submit them to the configured executor service)
        entriesFlux.parallel().runOn(Schedulers.fromExecutor(executor))

                ////////////////////////////////
                // processing logic goes here
                ////////////////////////////////

                // convert the entry to a string[] to be able to write it as xlsx
                .map(Entry::toStringArray)
                // switch back from parallel to sequential to be able to collect them
                .sequential()
                // collect all the modified rows into list<string[]>
                .collectList()
                .doOnNext(list -> log.info("Total processed entries {}", list.size()))
                // write the final xlsx file as byte[]
                .flatMap(XlsxUtils::write)
                // do anything with the bytes, persist it on the desk or push it other service e.g. FTP
                .zipWith(nameMono.map(name -> FileUtils.nameWithNewExtension(name, "xlsx")), this::persist)
                .subscribe(System.out::println);
    }

    private Mono<InputStream> reduce(Flux<DataBuffer> content) {
        return content.reduce(IOUtils.Input.empty(), IOUtils.Input.dataBufferInputStreamAccumulator());
    }

    private Mono<byte[]> unrar(ProcessorProperties props, java.io.InputStream inputStream) {
        return RarUtils.extract(props.getArchive().getTargetFileName(), inputStream, props.getArchive().getPassword());
    }

    private Boolean persist(byte[] bytes, String name) {
        try {

            FileOutputStream outputStream = new FileOutputStream(name);
            outputStream.write(bytes);
            outputStream.flush();
            outputStream.close();

            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
