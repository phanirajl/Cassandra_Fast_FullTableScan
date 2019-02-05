package com.cassandra.utility.method1;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;


public class ExecutionDataForTokenRange {
//    private final Cluster cluster;
    private final Session session;
    private final BoundStatement boundStatementRestOfTokenRange;
    private final BoundStatement boundStatementLastTokenRange;

    public ExecutionDataForTokenRange(Session session,String fetchStatementRestOfTokenRange, String fetchStatementLastTokenRange) {
        this.session = session;
        boundStatementRestOfTokenRange = new BoundStatement(session.prepare(fetchStatementRestOfTokenRange));
        boundStatementLastTokenRange = new BoundStatement(session.prepare(fetchStatementLastTokenRange));
    }

    public Session getSession() {
        return session;
    }

    public BoundStatement getBoundStatementRestOfTokenRange() {
        return boundStatementRestOfTokenRange;
    }

    public BoundStatement getBoundStatementLastTokenRange() {
        return boundStatementLastTokenRange;
    }
}
