package org.jetlinks.lettuce.codec;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
@EqualsAndHashCode
public class TestEntity implements TestInterface, Serializable {

    private String id;

    private int num;

    private Map<String,Object> nest;

    private Object data;
}
