package com.cassandra.utility.trial;

import com.datastax.driver.core.ResultSet;

import java.util.concurrent.LinkedBlockingQueue;


public class Consumer {
    private final LinkedBlockingQueue<ResultSet> queue;

    public Consumer(LinkedBlockingQueue<ResultSet> queue) {
        this.queue = queue;
    }
}
