package com.ibm.enpo.processor.utils;

import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * @author bishoybasily
 * @since 2020-08-16
 */
public class XlsxUtils {

    public static Flux<String[]> read(InputStream inputStream) {
        return Flux.create(sink -> {
            try {
                DataFormatter formatter = new DataFormatter();
                XSSFWorkbook workbook = new XSSFWorkbook(inputStream);

                workbook.forEach(sheet -> {
                    sheet.forEach(row -> {
                        int length = row.getPhysicalNumberOfCells();
                        String[] tokens = new String[length];
                        for (int i = 0; i < length; i++)
                            tokens[i] = formatter.formatCellValue(row.getCell(i));
                        sink.next(tokens);
                    });
                });

                sink.complete();

            } catch (IOException e) {
                sink.error(e);
            }
        });
    }

    public static Mono<byte[]> write(List<String[]> strings) {
        return Mono.fromCallable(() -> {
            Workbook workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet();

            /*
             * write cells' contents
             */

            for (int i = 0; i < strings.size(); i++) {
                Row row = sheet.createRow(i);
                for (int j = 0; j < strings.get(i).length; j++) {
                    row.createCell(j).setCellValue(strings.get(i)[j]);
                }
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);
            workbook.close();

            return outputStream.toByteArray();

        });
    }

}
