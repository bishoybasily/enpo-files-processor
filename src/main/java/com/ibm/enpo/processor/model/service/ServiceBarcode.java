package com.ibm.enpo.processor.model.service;

import com.ibm.enpo.processor.config.ProcessorProperties;
import com.ibm.enpo.processor.model.entity.Record;
import com.ibm.enpo.processor.utils.FileUtils;
import com.ibm.enpo.processor.utils.IOUtils;
import com.ibm.enpo.processor.utils.RarUtils;
import com.ibm.enpo.processor.utils.XlsxUtils;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * @author bishoybasily
 * @since 2020-08-27
 */
@Slf4j
@Service
@AllArgsConstructor
public class ServiceBarcode {

    private final ExecutorService executor;
    private final ProcessorProperties properties;

    /**
     * Extracts the uploaded file archive content then extract it into byte[] using {@link RarUtils#extract(String, InputStream, String)}
     * then generate a stream of line using {@link FileUtils#lines(byte[])}
     * and then creates a stream of {@link Record} using {@link Record#from(String)} to process them concurrently
     * with the help of the supplied {@link ExecutorService},
     * finally converts each of the processed record into a {@link String[]} representing a row in the final xlsx file.
     *
     * @param filePart the uploaded filePart
     * @return byte[] represents the final xlsx file
     */
    public Mono<byte[]> barcode(FilePart filePart) {

        return filePart.content()
                // reduce the file content input-stream into one single input-stream
                .reduce(IOUtils.Input.empty(), IOUtils.Input.dataBufferInputStreamAccumulator())
                // extract the compressed file into byte[]
                .flatMap(inputStream -> {
                    return RarUtils.extract(properties.getArchive().getTargetFileName(), inputStream, properties.getArchive().getPassword());
                })
                // read the lines as a stream
                .flatMapMany(FileUtils::lines)
                // map every line to an entry object
                .flatMap(Record::from)
                // run the next tasks in parallel (submit them to the configured executor service)
                .parallel().runOn(Schedulers.fromExecutor(executor))

                // simulate any processing
                .map(record -> {
                    return record.setValue(UUID.randomUUID().toString());
                })

                // convert the entry to a string[] to be able to write it as xlsx
                .map(Record::toStringArray)
                // switch back from parallel to sequential to be able to collect them
                .sequential()
                // collect all the modified rows into list<string[]>
                .collectList()
                .doOnNext(list -> log.info("Total processed entries {}", list.size()))
                // write the final xlsx file as byte[]
                .flatMap(XlsxUtils::write);

    }

}
