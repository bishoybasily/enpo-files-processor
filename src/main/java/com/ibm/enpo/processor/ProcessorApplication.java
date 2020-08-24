package com.ibm.enpo.processor;

import com.ibm.enpo.processor.utils.RarUtils;
import com.ibm.enpo.processor.utils.XlsxUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.boot.autoconfigure.SpringBootApplication;
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
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Consumer;

import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;

@SpringBootApplication
public class ProcessorApplication {

    public static void main(String[] args) throws Exception {

        FileInputStream inputStream = new FileInputStream("C:\\Users\\BishoyArmiaZaherBasi\\Desktop\\ENPO\\input.rar");
        String name = "Mailing file\\EVP-V233.OUT.OUT";
        String password = "Enpo";

        RarUtils.extract(name, inputStream, password)
                .subscribe(new Consumer<byte[]>() {
                    @Override
                    public void accept(byte[] bytes) {
                        writeTheFile(bytes, "temp");
                    }
                });


//        SpringApplication.run(ProcessorApplication.class, args);
    }

    @Bean
    public RouterFunction<ServerResponse> processor() {

        return RouterFunctions.route(
                POST("/processor").and(accept(MediaType.MULTIPART_FORM_DATA)),
                serverRequest -> {

                    Mono<FilePart> filePartMono = serverRequest
                            .body(BodyExtractors.toMultipartData())
                            .map(map -> (FilePart) map.toSingleValueMap().get("file")) // extract the uploaded multipart file from the form body
                            .cache(); // cache it for multiple consumers

                    Mono<String> nameMono = filePartMono.map(FilePart::filename) // get file's name
                            .cache(); // cache it for multiple consumers

                    filePartMono.map(FilePart::content) // get file's content
                            .flatMapMany(XlsxUtils::read) // read xlsx content (returns stream<string[]> where every single string[] represents a full row)
                            .parallel().runOn(Schedulers.parallel()) // run the next tasks in parallel
                            .flatMap(this::updateRow) // modify the string array (a dummy implementation that adds a new item to all the arrays)
                            .sequential() // switch back from parallel to sequential to be able to collect them
                            .collectList() // collect all the modified rows into list<string[]>
                            .flatMap(XlsxUtils::write) // write the contents into byte[] (the byte[] is ready to be persisted as file)
//                            .zipWith(nameMono, this::writeTheFile) // do anything with the byte[], (a dummy implementation that writes the file to the disk)
                            .subscribe();

                    /*
                     * for the http part return a response shows that the file has been received and it is being processed.
                     */
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(nameMono.map(Response::new), Response.class);

                }
        );
    }

    private static Boolean writeTheFile(byte[] bytes, String name) {
        try {

//            String path = "/home/bishoybasily/Desktop/";

            IOUtils.write(bytes, new FileOutputStream( name));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private Mono<String[]> updateRow(String[] strings) {
        return Mono.fromCallable(() -> {
            String[] result = Arrays.copyOf(strings, strings.length + 1);
            result[strings.length] = UUID.randomUUID().toString();
            return result;
        });
    }

}
