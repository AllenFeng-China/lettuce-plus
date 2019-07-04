package org.jetlinks.lettuce;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
public class ServerNodeInfo implements Serializable {

    private String id;

    private State state;

    private Map<String,Object> properties;

    public ServerNodeInfo(String id){
        this.id=id;
    }

    public enum State{
        ONLINE,OFFLINE
    }

}
