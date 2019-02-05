package com.cassandra.utility.method1;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;


/*public*/ class Producer extends Thread {
    private final Set<TokenRange> tokenRangeSetForProducer;
    private final Session session;
    private final CountDownLatch latch;
    private final LinkedBlockingQueue</*ResultSet*/Row> personalQueue;
    private final LinkedBlockingQueue<Row> mainQueue;
    private final Thread dedicatedConsumer;
    private final BoundStatement boundStatementRestOfTokenRange;
    private final BoundStatement boundStatementLastTokenRange;
    private static ConsistencyLevel consistencyLevel;
    private static int sleepMilliSeconds;
    protected static boolean printDebugStatements;
    protected static PrintStream loggingFile;
    private final int fetchSize;
    private final Long startValueOfToken;

    protected static void  setStaticData(ConsistencyLevel consistencyLevel, int sleepMilliSeconds,PrintStream loggingFile){
        DedicatedConsumer.setStaticData(loggingFile);
        Producer.consistencyLevel = consistencyLevel;
        Producer.sleepMilliSeconds = sleepMilliSeconds;
        Producer.printDebugStatements = true;
        Producer.loggingFile = loggingFile;
    }



    protected Producer(String threadName,Set<TokenRange> tokenRangeSetForProducer, Session session/*Cluster cluster*/, CountDownLatch latch, LinkedBlockingQueue mainQueue, int personalQueueSize,String fetchStatementRestOfTokenRange,String fetchStatementLastTokenRange,int fetchSize,Long startValueOfToken) {
        super(threadName);
        loggingFile.println("TOKEN_PERSONAL_RANGE:"+tokenRangeSetForProducer.size()+ "" +Thread.currentThread().getName());
        this.tokenRangeSetForProducer = tokenRangeSetForProducer;
        this.session = /*cluster.connect()*/session;
        this.latch = latch;
        this.mainQueue = mainQueue;
        this.personalQueue = new LinkedBlockingQueue(personalQueueSize);
        this.boundStatementRestOfTokenRange = new BoundStatement(session.prepare(fetchStatementRestOfTokenRange));
        boundStatementLastTokenRange = new BoundStatement(session.prepare(fetchStatementLastTokenRange));

        this.fetchSize = fetchSize;
        this.dedicatedConsumer = new DedicatedConsumer("DedicatedConsumer_FOR_"+threadName,personalQueue,this.mainQueue,this.latch);
        this.startValueOfToken = startValueOfToken;
    }

    public void run(){
        dedicatedConsumer.start();
        for(TokenRange tokenRange : tokenRangeSetForProducer){
            if(printDebugStatements) loggingFile.println("TOKEN_RANGE : "+tokenRange.getStart()+";"+tokenRange.getEnd()+ " " +Thread.currentThread().getName());
            fetchLoopWithAutomaticPaging(tokenRange);
        }
        session.close();
        try{
            personalQueue.put(/*new ProducerEnd()*//*null*/new RowTerminal());
        }catch (InterruptedException e){
            loggingFile.println("Interrupted Excpetion while pushing terminal value to personalQueue. "+Thread.currentThread().getName()+" thread.");
            System.exit(1);
        }

        try{
            dedicatedConsumer.join();
        }catch (InterruptedException e){
            loggingFile.println("Dedicated consumer interrupted for "+Thread.currentThread().getName()+" thread. Join failed");
            System.exit(1);
        }

//        try{
//            latch.await();
//        }catch (InterruptedException e){
//            System.out.println("Latch interrupted. "+Thread.currentThread().getName()+" thread.");
//            System.exit(1);
//        }
    }

    //works.
    //but try to make manualpaging work
    //it might be faster
    private void fetchLoopWithAutomaticPaging(TokenRange tokenRange){
        BoundStatement boundStatement;
        if((Long)tokenRange.getStart().getValue() > (Long)tokenRange.getEnd().getValue()){
            /*
            Case
            9219444290392454365 to -9207785194558378121
             */
            boundStatement = boundStatementLastTokenRange;
            boundStatement.bind(tokenRange.getStart().getValue());
            fetchLoopForBoundStatement(session,boundStatement);
            boundStatement = boundStatementRestOfTokenRange;
            boundStatement.bind(Long.MIN_VALUE,startValueOfToken);
            fetchLoopForBoundStatement(session,boundStatement);
        }else{
            boundStatement = boundStatementRestOfTokenRange;
            boundStatement.bind(tokenRange.getStart().getValue(),tokenRange.getEnd().getValue());
            fetchLoopForBoundStatement(session,boundStatement);
        }



    }

    private void fetchLoopForBoundStatement(Session session,BoundStatement boundStatement) {
        boundStatement.setConsistencyLevel(consistencyLevel);
        boundStatement.setFetchSize(/*10*//*5000*/fetchSize);
        //add retry logic, if needed
        ResultSet resultSet = session.execute(boundStatement);
        boolean completed = false;
        Token lastProcessedToken=null;
        while(!completed) {
            try {
                for (Row row : resultSet) {
                    try {
//                        lastProcessedToken = row.getPartitionKeyToken();
                        personalQueue.put(row);
                    } catch (InterruptedException e) {
                        loggingFile.println("Interrupted Exception while pushing row to personal queue." + Thread.currentThread().getName() + " thread.");
                        System.exit(1);
                    }
                }
                completed = true;
            }catch (Exception e){
                loggingFile.println("READ EXCEPTION." + Thread.currentThread().getName() + " thread. Trying again."+e);
                e.printStackTrace();
//                boundStatement.bind((long)lastProcessedToken.getValue()+1,tokenRange.getEnd().getValue());
                resultSet = session.execute(boundStatement);
                try{Thread.sleep(1000);}catch (Exception e1){}
            }
        }
    }

    private void fetchLoopWithManualPaging(TokenRange tokenRange) {
        //doesn't work
        //fails.
        /*
        Exception in thread "Producer-0" com.datastax.driver.core.exceptions.ServerError: An unexpected error occurred server side on /30.0.229.167:9042: java.lang.AssertionError: [DecoratedKey(-5837859558714651203, 53444c343130343438333136),min(-8551554565574869962)]
            at com.datastax.driver.core.exceptions.ServerError.copy(ServerError.java:63)
            at com.datastax.driver.core.exceptions.ServerError.copy(ServerError.java:25)
            at com.datastax.driver.core.DriverThrowables.propagateCause(DriverThrowables.java:37)
            at com.datastax.driver.core.DefaultResultSetFuture.getUninterruptibly(DefaultResultSetFuture.java:245)
            at com.datastax.driver.core.AbstractSession.execute(AbstractSession.java:64)
            at com.cassandra.utility.Producer.fetchLoop(Producer.java:84)
            at com.cassandra.utility.Producer.run(Producer.java:48)
        Caused by: com.datastax.driver.core.exceptions.ServerError: An unexpected error occurred server side on /30.0.229.167:9042: java.lang.AssertionError: [DecoratedKey(-5837859558714651203, 53444c343130343438333136),min(-8551554565574869962)]
            at com.datastax.driver.core.Responses$Error.asException(Responses.java:108)
            at com.datastax.driver.core.RequestHandler$SpeculativeExecution.onSet(RequestHandler.java:500)
            at com.datastax.driver.core.Connection$Dispatcher.channelRead0(Connection.java:1012)
            at com.datastax.driver.core.Connection$Dispatcher.channelRead0(Connection.java:935)
            at io.netty.channel.SimpleChannelInboundHandler.channelRead(SimpleChannelInboundHandler.java:105)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:342)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:328)
            at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:321)
            at io.netty.handler.timeout.IdleStateHandler.channelRead(IdleStateHandler.java:266)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:342)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:328)
            at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:321)
            at io.netty.handler.codec.MessageToMessageDecoder.channelRead(MessageToMessageDecoder.java:102)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:342)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:328)
            at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:321)
            at io.netty.handler.codec.ByteToMessageDecoder.fireChannelRead(ByteToMessageDecoder.java:293)
            at io.netty.handler.codec.ByteToMessageDecoder.channelRead(ByteToMessageDecoder.java:267)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:342)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:328)
            at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:321)
            at io.netty.channel.DefaultChannelPipeline$HeadContext.channelRead(DefaultChannelPipeline.java:1280)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:342)
            at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:328)
            at io.netty.channel.DefaultChannelPipeline.fireChannelRead(DefaultChannelPipeline.java:890)
            at io.netty.channel.nio.AbstractNioByteChannel$NioByteUnsafe.read(AbstractNioByteChannel.java:131)
            at io.netty.channel.nio.NioEventLoop.processSelectedKey(NioEventLoop.java:564)
            at io.netty.channel.nio.NioEventLoop.processSelectedKeysOptimized(NioEventLoop.java:505)
            at io.netty.channel.nio.NioEventLoop.processSelectedKeys(NioEventLoop.java:419)
            at io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:391)
            at io.netty.util.concurrent.SingleThreadEventExecutor$2.run(SingleThreadEventExecutor.java:112)
            at java.lang.Thread.run(Thread.java:745)

         */
        BoundStatement boundStatement = null;
        boundStatement.bind(tokenRange.getStart().getValue(),tokenRange.getEnd().getValue());
        boundStatement.setConsistencyLevel(consistencyLevel);
        boundStatement.setFetchSize(10);
        String currentPageInfo = null;
//        boolean oneFetchDone = false;
        do {
            try {
                if(printDebugStatements) loggingFile.println("Hitting..." + currentPageInfo + "..."+Thread.currentThread().getName()+" thread.");
                if (currentPageInfo != null) {
                    boundStatement.setPagingState(PagingState.fromString(currentPageInfo));
                }
                ResultSet rs = session.execute(boundStatement);
//                oneFetchDone = true;
                if(printDebugStatements) loggingFile.println("Pushed to queue");
                try{
                    personalQueue.put(/*rs*/null);
                }catch (InterruptedException e){
                    loggingFile.println("Interrupted Exception while pushing result set to personal queue."+Thread.currentThread().getName()+" thread.");
                    System.exit(1);
                }
                PagingState nextPage = rs.getExecutionInfo().getPagingState();
                String nextPageInfo = null;
                if (nextPage != null) {
                    nextPageInfo = nextPage.toString();
                }
                currentPageInfo = nextPageInfo;
            } catch (NoHostAvailableException e) {
                if(printDebugStatements) loggingFile.println("No host available exception... going to sleep for 1 sec"+Thread.currentThread().getName()+" thread.");
                try {Thread.sleep(sleepMilliSeconds);} catch (Exception e2) {/*nothing*/}
            }
            if(printDebugStatements) loggingFile.println("Finished while loop"+Thread.currentThread().getName()+" thread.");
        } while (/*(!oneFetchDone && currentPageInfo == null) ||*//*(!oneFetchDone)||*/(currentPageInfo != null));
    }
}
