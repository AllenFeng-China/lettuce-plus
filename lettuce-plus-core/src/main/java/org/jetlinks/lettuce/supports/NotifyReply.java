package org.jetlinks.lettuce.supports;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor(staticName = "of")
public class NotifyReply implements Serializable {

    private String notifyId;

    private String address;

    private String payloadType;

    private Object payload;

    private boolean success;

    private String errorType;

    private String errorMessage;

    public static NotifyReply of(Notify notify, Object payload, Throwable e) {
        NotifyReply reply = new NotifyReply();
        reply.setAddress(notify.getAddress());
        reply.setNotifyId(notify.getNotifyId());
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