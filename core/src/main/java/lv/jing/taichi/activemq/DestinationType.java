package lv.jing.taichi.activemq;

/**
 * Destination Type for activemq
 **/
public enum DestinationType {

    QUEUE("queue"),TOPIC("topic");
    
    private final String value;
    
    DestinationType(String value){
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
