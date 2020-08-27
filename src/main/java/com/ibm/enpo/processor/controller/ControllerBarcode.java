package com.ibm.enpo.processor.controller;

import com.ibm.enpo.processor.config.ProcessorProperties;
import com.ibm.enpo.processor.model.service.ServiceBarcode;
import com.ibm.enpo.processor.utils.FileUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestPart;

import java.io.FileOutputStream;
import java.net.URI;

/**
 * @author bishoybasily
 * @since 2020-08-27
 */
@Slf4j
@RequiredArgsConstructor
@Controller
public class ControllerBarcode {

    private final ProcessorProperties properties;
    private final ServiceBarcode serviceBarcode;

    @PostMapping(value = "/barcode", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Object> barcode(@RequestPart FilePart file) {

        serviceBarcode.barcode(file)
                .subscribe(bytes -> {

                    String name = FileUtils.nameWithNewExtension(file.filename(), "xlsx");

                    try {

                        FileOutputStream outputStream = new FileOutputStream(name);
                        outputStream.write(bytes);
                        outputStream.flush();
                        outputStream.close();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                });

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setLocation(URI.create(properties.getRedirectUri()));
        return new ResponseEntity<>(httpHeaders, HttpStatus.SEE_OTHER);
    }

}
