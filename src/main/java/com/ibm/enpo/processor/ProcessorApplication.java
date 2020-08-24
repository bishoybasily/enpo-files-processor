package com.ibm.enpo.processor;

import com.ibm.enpo.processor.model.Entry;
import com.ibm.enpo.processor.utils.FileUtils;
import com.ibm.enpo.processor.utils.RarUtils;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.UUID;

import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;

@SpringBootApplication
public class ProcessorApplication {

    public static void main(String[] args) throws Exception {

        FileInputStream inputStream = new FileInputStream("C:\\Users\\BishoyArmiaZaherBasi\\Desktop\\ENPO\\input.rar");
        String name = "Mailing file\\EVP-V233.OUT.OUT";
        String password = "Enpo";

        RarUtils.extract(name, inputStream, password)
                .flatMapMany(FileUtils::lines)
                .flatMap(line -> {
                    return Mono.fromCallable(() -> {
                        String a = line.substring(0, 20).trim();
                        String b = line.substring(20, 44).trim();
                        String c = line.substring(44, 69).trim();
                        String d = line.substring(69, 78).trim();
                        return new Entry().setCol1(a).setCol2(b).setCol3(c).setCol4(d);
                    }).onErrorResume(it -> Mono.empty());
                })
                .subscribe(entry -> {
                    System.out.println(entry);
                });


//        SpringApplication.run(ProcessorApplication.class, args);
    }

    private static Boolean writeTheFile(byte[] bytes, String name) {
        try {

//            String path = "/home/bishoybasily/Desktop/";

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

//                    filePartMono.map(FilePart::content) // get file's content
//                            .flatMap(content -> content.reduce(IOUtils.Input.empty(), IOUtils.Input.dataBufferInputStreamAccumulator())) // reduce the file content input-stream into one single input-stream
//                            .flatMapMany(inputStream -> RarUtils.extract("", inputStream, "")) // read xlsx content (returns stream<string[]> where every single string[] represents a full row)
//                            .parallel().runOn(Schedulers.parallel()) // run the next tasks in parallel
//                            .flatMap(this::updateRow) // modify the string array (a dummy implementation that adds a new item to all the arrays)
//                            .sequential() // switch back from parallel to sequential to be able to collect them
//                            .collectList() // collect all the modified rows into list<string[]>
//                            .flatMap(XlsxUtils::write) // write the contents into byte[] (the byte[] is ready to be persisted as file)
////                            .zipWith(nameMono, this::writeTheFile) // do anything with the byte[], (a dummy implementation that writes the file to the disk)
//                            .subscribe();

                    /*
                     * for the http part return a response shows that the file has been received and it is being processed.
                     */
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(nameMono.map(Response::new), Response.class);

                }
        );
    }

    private Mono<String[]> updateRow(String[] strings) {
        return Mono.fromCallable(() -> {
            String[] result = Arrays.copyOf(strings, strings.length + 1);
            result[strings.length] = UUID.randomUUID().toString();
            return result;
        });
    }

}
