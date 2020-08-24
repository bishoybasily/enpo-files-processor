package com.ibm.enpo.processor;

import com.ibm.enpo.processor.config.ProcessorProperties;
import com.ibm.enpo.processor.model.Entry;
import com.ibm.enpo.processor.utils.FileUtils;
import com.ibm.enpo.processor.utils.IOUtils;
import com.ibm.enpo.processor.utils.RarUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.FileOutputStream;
import java.util.concurrent.ExecutorService;

import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;

@ConfigurationPropertiesScan
@SpringBootApplication
public class ProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProcessorApplication.class, args);
    }

    private static Boolean writeTheFile(byte[] bytes, String name) {
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

    @Bean
    public RouterFunction<ServerResponse> processor(ProcessorProperties props, ExecutorService executor) {

        return RouterFunctions.route(
                POST("/processor").and(accept(MediaType.MULTIPART_FORM_DATA)),
                serverRequest -> {

                    Mono<FilePart> filePartMono = serverRequest.body(BodyExtractors.toMultipartData())
                            .map(map -> (FilePart) map.toSingleValueMap().get("file")) // extract the uploaded multipart file from the form body
                            .cache(); // cache it for multiple consumers

                    Mono<String> nameMono = filePartMono.map(FilePart::filename) // get file's name
                            .cache(); // cache it for multiple consumers

                    filePartMono.map(FilePart::content) // get file's content
                            .flatMap(content -> content.reduce(IOUtils.Input.empty(), IOUtils.Input.dataBufferInputStreamAccumulator())) // reduce the file content input-stream into one single input-stream
                            .flatMapMany(inputStream -> RarUtils.extract(props.getArchive().getTargetFileName(), inputStream, props.getArchive().getPassword())) // read xlsx content (returns stream<string[]> where every single string[] represents a full row)
                            .parallel().runOn(Schedulers.fromExecutor(executor)) // run the next tasks in parallel (submit them to the configured executor service)
                            .flatMap(FileUtils::lines)
                            .flatMap(Entry::from)

//                            .sequential() // switch back from parallel to sequential to be able to collect them
//                            .collectList() // collect all the modified rows into list<string[]>
                            .subscribe(entry -> System.out.println(entry));

                    /*
                     * for the http part return a response shows that the file has been received and it is being processed.
                     */
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(nameMono.map(Response::new), Response.class);

                }
        );
    }

}
