package org.apache.kafka.connect.mixpanel;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is the actual client that makes requests to the Mixpanel API. It uses Unirest together with Java 8 futures to make it non-blocking.
 * It is used by the Connector Task. Currently potential errors are handled in a very naive way, the client just returns back with an empty
 * queue. Some error handling logic should be incorporated in the communication protocol between the client and the Task threads. Apart from that
 * the class is quite simple.
 * Created by Kostas.
 */
public class MixPanelClient implements Runnable {

    private BlockingQueue queue;
    private AtomicBoolean done;
    private String endpoint;
    private String api_key;
    private String from_date;
    private String to_date;
    private String sig;
    private int expire;


    public MixPanelClient(BlockingQueue<String> queue, AtomicBoolean done, String endpoint, String api_key, String from_date, String to_date, String sig, int expire){
        this.queue = queue;
        this.done = done;

        this.endpoint = endpoint;
        this.api_key = api_key;
        this.from_date = from_date;
        this.to_date = to_date;
        this.sig = sig;
        this.expire = expire;
    }

    @Override
    public void run() {

        Future<HttpResponse<String>> future = Unirest.get(endpoint).queryString("api_key", api_key).queryString("from_date", from_date).
                queryString("to_date", to_date).queryString("expire", String.valueOf(expire)).queryString("sig", sig).asStringAsync(new Callback<String>() {

            public void failed(UnirestException e) {
                    e.printStackTrace();
                }

            public void completed(HttpResponse<String> response) {

                if(response.getStatus() == 200) {
                    String body = response.getBody();
                    String[] strs = body.split("\n");
                    for (String str : strs) {
                        try {
                            queue.put(str);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    }
                }
                // need to figure out a way to communicate errors back to the Task
                done.set(true);

            }

            public void cancelled() {
                    System.out.println("The request has been cancelled");
                }

        });
    }
}
