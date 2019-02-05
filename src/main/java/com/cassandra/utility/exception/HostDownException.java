package com.cassandra.utility.exception;

import com.datastax.driver.core.Host;


public class HostDownException extends RuntimeException {
    public HostDownException(Host hostName){
        super(hostName.getAddress()+" is down");
    }
}
