package com.ibm.enpo.processor;

import lombok.SneakyThrows;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;

/**
 * @author bishoybasily
 * @since 2020-08-16
 */
public class XlsxUtils {

    public static Flux<String[]> read(Flux<DataBuffer> content) {

        DataFormatter formatter = new DataFormatter();

        return content.reduce(StreamUtils.empty(), StreamUtils.dataBufferInputStreamAccumulator()) // reduce the file content input-stream into one single input-stream
                .flatMapIterable(XlsxUtils::createWorkbook) // create empty workbook (iterable of sheets)
                .flatMapIterable(rows -> rows) // map the rows as it is (iterable of rows)
                .skip(0) // we can skip rows here if there is any header rows
                .map(row -> {

                    /*
                     * read cells' contents
                     */

                    int length = row.getPhysicalNumberOfCells();
                    String[] tokens = new String[length];
                    for (int i = 0; i < length; i++)
                        tokens[i] = formatter.formatCellValue(row.getCell(i));
                    return tokens;
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

    @SneakyThrows
    private static XSSFWorkbook createWorkbook(InputStream inputStream) {
        return new XSSFWorkbook(inputStream);
    }

}
