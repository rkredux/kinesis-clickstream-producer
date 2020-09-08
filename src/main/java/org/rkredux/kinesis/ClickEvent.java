package org.rkredux.kinesis;

public class ClickEvent {

    private String sessionId;
    private String payload;

    public ClickEvent(String sessionId, String payload){
        this.sessionId = sessionId;
        this.payload = payload;
    }

    public String getSessionId(){
        return this.sessionId;
    }

    public String getPayload(){
        return this.payload;
    }

}