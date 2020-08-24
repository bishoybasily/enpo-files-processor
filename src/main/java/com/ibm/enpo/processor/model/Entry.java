package com.ibm.enpo.processor.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author bishoybasily
 * @since 2020-08-24
 */
@Data
@Accessors(chain = true)
public class Entry {

    private String col1, col2, col3, col4;

}
