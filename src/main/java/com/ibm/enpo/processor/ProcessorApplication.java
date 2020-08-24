package com.ibm.enpo.processor;

import com.ibm.enpo.processor.config.ProcessorProperties;
import com.ibm.enpo.processor.model.Entry;
import com.ibm.enpo.processor.utils.FileUtils;
import com.ibm.enpo.processor.utils.IOUtils;
import com.ibm.enpo.processor.utils.RarUtils;
import lombok.extern.slf4j.Slf4j;
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

import java.io.FileInputStream;
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

    public static void main(String[] args) throws Exception {

        InputStream inputStream = new FileInputStream("C:\\Users\\BishoyArmiaZaherBasi\\Desktop\\ENPO\\input.rar");
        String targetFileName = "Mailing file\\EVP-V233.OUT.OUT";
        String password = "Enpo";
        List<String> block = RarUtils.extract(targetFileName, inputStream, password)
                .flatMapMany(FileUtils::lines)
//                .flatMap(Entry::from)
//                .map(Entry::toStringArray)
                .collectList()
                .block();


        System.out.println(block.size());

//        SpringApplication.run(ProcessorApplication.class, args);
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
                            .flatMap(inputStream -> RarUtils.extract(props.getArchive().getTargetFileName(), inputStream, props.getArchive().getPassword())) // read xlsx content (returns stream<string[]> where every single string[] represents a full row)
                            .flatMapMany(FileUtils::lines)
                            .parallel().runOn(Schedulers.fromExecutor(executor)) // run the next tasks in parallel (submit them to the configured executor service)
                            .flatMap(Entry::from)
                            .map(Entry::toStringArray)
                            .sequential()
                            .collectList()
                            .subscribe(it -> {

                                System.out.println(it.size());

                            });

//                            .doOnNext(entry -> log.info("Started working on {}", entry))
//                            // processing logic goes here
//                            .map(Entry::toStringArray);
//                            .doOnNext(strings -> log.info("Done working with {}", Arrays.toString(strings)))
//                            .sequential() // switch back from parallel to sequential to be able to collect them
//                            .collectList() // collect all the modified rows into list<string[]>
//                            .subscribe(strings -> {
//                                int size = strings.size();
//                                System.out.println(size);
//                            });
//                            .doOnNext(list -> log.info("Total processed entries {}", list.size()))
//                            .flatMap(XlsxUtils::write)
//                            .subscribe(bytes -> writeTheFile(bytes, "output.xlsx"));

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
