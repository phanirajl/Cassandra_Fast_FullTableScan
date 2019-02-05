package com.cassandra.utility.method1;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class CassandraFastFullTableScan {
    private final String tableIdentifier;
    private final String contactPoint;
    private final String username;
    private final String password;
    private final ConsistencyLevel consistencyLevel;
    private final int numberOfThreads;
    private final LinkedBlockingQueue<Row> resultQueue;
    private final String dc;
    private final String partitionKey;
    private final String keyspace;
    private final String tableName;
    private final Thread producerThreads[];
    private final CountDownLatch latch;
    private final int personalQueueSizePerProducer;
    private final ArrayList<String> columns;
    private final int sleepMilliSeconds;
    private final int fetchSize;
    private final PrintStream loggingFile;// we can use log4j as well;
//    private final boolean enableWhiteListPolicy;

    public CassandraFastFullTableScan(String tableIdentifier, String contactPoint,LinkedBlockingQueue<Row> resultQueue, Options options/*, boolean enableWhiteListPolicy*/,PrintStream loggingFile) {
        this.loggingFile = loggingFile;
        loggingFile.println(options);
        this.tableIdentifier = tableIdentifier;
        this.contactPoint = contactPoint;
        this.resultQueue = resultQueue;
        this.username = options.getUsername();
        this.password = options.getPassword();
        this.consistencyLevel = options.getConsistencyLevel()/*ConsistencyLevel.ALL*/;
        this.dc = options.getDc();
//        this.enableWhiteListPolicy=enableWhiteListPolicy/*false*/;
        LoadBalancingPolicy loadBalancingPolicy;
        if(dc != null){
            loadBalancingPolicy = DCAwareRoundRobinPolicy.builder().withLocalDc(dc).build();
        }else{
            loadBalancingPolicy = new RoundRobinPolicy();
        }
        Cluster cluster = Cluster.builder().addContactPoint(contactPoint)
                        .withQueryOptions(new QueryOptions().setFetchSize(5000))
                        .withCredentials(username,password)
                        .withSocketOptions(new SocketOptions().setReadTimeoutMillis(1000 * 60 * 60).setConnectTimeoutMillis(1000 * 60 * 60))
                        .withLoadBalancingPolicy(new TokenAwarePolicy(loadBalancingPolicy))
                        .build();
      
        //get partition key.
        //not tested on composite partition key
        String temp[] = tableIdentifier.split("\\.");
        this.keyspace = temp[0];
        this.tableName = temp[1];
        this.partitionKey = getPartitionKey(cluster);
        this.columns = options.getColumnNames();
        this.fetchSize = options.getFetchSize();
        this.personalQueueSizePerProducer = options.getPersonalQueueSize();
        this.sleepMilliSeconds = options.getSleepMilliSeconds();
        Producer.setStaticData(consistencyLevel,sleepMilliSeconds,loggingFile);
        this.numberOfThreads = options.getNumberOfThreads();
        this.latch = new CountDownLatch(numberOfThreads);
        this.producerThreads = readyProducers(cluster);
        new Thread(){
          @Override
            public void run(){
              try{latch.await();}catch (Exception e){}
              try{
                  resultQueue.put(new RowTerminal());
              }catch (Exception e1){
                  //what to do.
              }
              cluster.close();
          }
        }.start();
    }

    private Thread[] readyProducers(Cluster cluster) {
        Thread producerThreads[] = new Thread[numberOfThreads];
        Set<TokenRange> tokenRangeSetForOneProducer = new HashSet<>();
        int numberOfTokensPerConsumer = cluster.getMetadata().getTokenRanges().size() / numberOfThreads;
        int numberOfProducersWithExtraToken = cluster.getMetadata().getTokenRanges().size() % numberOfThreads;
        int tokensAddedForCurrentConsumer = 0;
        int indexOfProducer = 0;
        StringBuffer selectionColumnsBuffer = new StringBuffer();
        //Why using StringBuffer when I can use StringBuilder? Don't know! :D P.S. if you reading this, you go coder!!! You do you! You do you!!!:*
        for(String column : columns){
            selectionColumnsBuffer.append(column+",");
        }
        String selectionColumns = selectionColumnsBuffer.substring(0,selectionColumnsBuffer.length()-1);

        /*
        tokenRange :
        1.      -9207785194558378121 to -9204547573912250796
        2.      -9204547573912250796 to -9199054268853034612
        ...
        1536.   9219444290392454365 to -9207785194558378121
        Thus, start inclusive, end exclusive
         */
        String fetchStatement = "select "+selectionColumns+" from "+keyspace+ "." +tableName +" where token("+partitionKey+") >= ? and token("+partitionKey+") < ? ";
        String fetchStatementLastTokenRange = "select "+selectionColumns+" from "+keyspace+ "." +tableName +" where token("+partitionKey+") >= ?";//  and token("+partitionKey+") <= ? ";
        //I think, this is required. Not sure yet, open for discussion.

        /*
        Example 1:
        token range size : 38
        Producers : 7
        1   2  3  4  5  6
        7   8  9 10 11 12
        13 14 15 16 17 18
        19 20 21 22 23
        24 25 26 27 28
        29 30 31 32 33
        34 35 36 37 38
        Example 2:
        token range size : 16
        Producers : 2
        1  2  3  4  5  6  7  8
        9 10 11 12 13 14 15 16
         */
        loggingFile.println("TOKEN_PERSONAL_RANGE:"+cluster.getMetadata().getTokenRanges().size()+" FULL COUNT.");
        /*
            tried this when manual paging gave error.
            Still not resolved
            Cluster personalCluster = Cluster.builder().addContactPoint(contactPoint)
                .withQueryOptions(new QueryOptions().setFetchSize(5000))
                .withCredentials(username,password)
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(1000 * 60 * 60).setConnectTimeoutMillis(1000 * 60 * 60))
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                .build();*/
        TreeSet<TokenRange> tokenRanges = new TreeSet<>(cluster.getMetadata().getTokenRanges());
        Long startValueOfToken = (Long)(tokenRanges.first().getStart().getValue());

        for(TokenRange tokenRange : tokenRanges){
            if(tokensAddedForCurrentConsumer == numberOfTokensPerConsumer){
                if(numberOfProducersWithExtraToken== 0) {
                    producerThreads[indexOfProducer] = new Producer("Producer-"+indexOfProducer,tokenRangeSetForOneProducer,cluster.newSession()/*personalCluster*/,latch,resultQueue,personalQueueSizePerProducer,fetchStatement,fetchStatementLastTokenRange,fetchSize,startValueOfToken);
                    /*
                    tried this when manual paging gave error.
                    Still not resolved
                    personalCluster = Cluster.builder().addContactPoint(contactPoint)
                            .withQueryOptions(new QueryOptions().setFetchSize(5000))
                            .withCredentials(username,password)
                            .withSocketOptions(new SocketOptions().setReadTimeoutMillis(1000 * 60 * 60).setConnectTimeoutMillis(1000 * 60 * 60))
                            .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                            .build();*/
                    tokenRangeSetForOneProducer = new HashSet<>();
                    tokenRangeSetForOneProducer.add(tokenRange);
                    tokensAddedForCurrentConsumer = 1;
                    ++indexOfProducer;
                }else{
                    tokenRangeSetForOneProducer.add(tokenRange);
                    producerThreads[indexOfProducer] = new Producer("Producer-"+indexOfProducer,tokenRangeSetForOneProducer,cluster.newSession()/*personalCluster*/,latch,resultQueue,personalQueueSizePerProducer,fetchStatement,fetchStatementLastTokenRange,fetchSize,startValueOfToken);
                    /*
                    tried this when manual paging gave error.
                    Still not resolved
                    personalCluster = Cluster.builder().addContactPoint(contactPoint)
                            .withQueryOptions(new QueryOptions().setFetchSize(5000))
                            .withCredentials(username,password)
                            .withSocketOptions(new SocketOptions().setReadTimeoutMillis(1000 * 60 * 60).setConnectTimeoutMillis(1000 * 60 * 60))
                            .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                            .build();*/
                    --numberOfProducersWithExtraToken;
                    tokenRangeSetForOneProducer = new HashSet<>();
                    tokensAddedForCurrentConsumer = 0;
                    ++indexOfProducer;
                }
            }else{
                tokenRangeSetForOneProducer.add(tokenRange);
                ++tokensAddedForCurrentConsumer;
            }
        }
        producerThreads[indexOfProducer] = new Producer("Producer-"+indexOfProducer,tokenRangeSetForOneProducer,cluster.newSession()/*personalCluster*/,latch,resultQueue,personalQueueSizePerProducer,fetchStatement,fetchStatementLastTokenRange,fetchSize,startValueOfToken);

        return producerThreads;
    }

    public CountDownLatch start(){
        loggingFile.println("CFS Started");
        for(Thread thread : producerThreads){
            thread.start();
        }
        return latch;

    }

    private String getPartitionKey(Cluster cluster) {
        StringBuffer partitionKeyTemp = new StringBuffer();
        for(ColumnMetadata partitionKeyPart : cluster.getMetadata().getKeyspace(keyspace).getTable(tableName).getPartitionKey()){
            partitionKeyTemp.append(partitionKeyPart.getName()+",");
        }
        String partitionKey = partitionKeyTemp.substring(0,partitionKeyTemp.length()-1);
        return partitionKey;
    }
}
