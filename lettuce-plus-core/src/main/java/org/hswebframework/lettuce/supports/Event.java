package org.hswebframework.lettuce.supports;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor(staticName = "of")
public class Event implements Serializable {

    private String eventId;

    private String address;

    private String payloadType;

    private Object payload;

}