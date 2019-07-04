package org.jetlinks.lettuce.supports;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor(staticName = "of")
public class EventReply implements Serializable {

    private String eventId;

    private String address;

    private String payloadType;

    private Object payload;

    private boolean success;

    private String errorType;

    private String errorMessage;

    public static EventReply of(Event event, Object payload, Throwable e) {
        EventReply reply = new EventReply();
        reply.setAddress(event.getAddress());
        reply.setEventId(event.getEventId());
        if (payload != null) {
            reply.setPayload(payload);
            reply.setPayloadType(payload.getClass().getName());
            reply.setSuccess(true);
        }
        reply.setPayload(payload);
        if (e != null) {
            reply.setErrorType(e.getClass().getName());
            reply.setErrorMessage(e.getMessage());
            reply.setSuccess(false);
        }
        return reply;
    }
}