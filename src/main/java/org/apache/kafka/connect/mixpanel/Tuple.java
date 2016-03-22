package org.apache.kafka.connect.mixpanel;

/**
 * Simple Java object that represents a Tuple. Useful for handling the date intervals that we use in the Connector
 * Created by Kostas.
 */
public class Tuple<X,Y> {

    public final X x;
    public final Y y;

    public Tuple(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public String toString(){
        return "(" + x + "," + y +")";
    }
}
