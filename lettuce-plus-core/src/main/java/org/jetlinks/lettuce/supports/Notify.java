package org.jetlinks.lettuce.supports;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor(staticName = "of")
public class Notify implements Serializable {

    private String fromServer;

    private String notifyId;

    private String address;

    private String payloadType;

    private Object payload;

}