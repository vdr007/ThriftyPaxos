/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import glassPaxos.client.Client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.h2.engine.SessionRemote;
import org.h2.engine.SysProperties;
import org.h2.expression.ParameterInterface;
import org.h2.expression.ParameterRemote;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.ResultInterface;
import org.h2.result.ResultRemote;
import org.h2.util.New;
import org.h2.value.ByteBufferTool;
import org.h2.value.Transfer;
import org.h2.value.Value;

/**
 * Represents the client-side part of a SQL statement.
 * This class is not used in embedded mode.
 */
public class CommandRemote implements CommandInterface {

    private final ArrayList<Client> transferList;
    private final ArrayList<ParameterInterface> parameters;
    private final Trace trace;
    private final String sql;
    private final int fetchSize;
    private SessionRemote session;
    private int id;
    private boolean isQuery;
    private boolean readonly;
    private final int created;

    public CommandRemote(SessionRemote session,
            ArrayList<Client> transferList, String sql, int fetchSize) {
        this.transferList = transferList;
        trace = session.getTrace();
        this.sql = sql;
        parameters = New.arrayList();
        prepare(session, true);
        // set session late because prepare might fail - in this case we don't
        // need to close the object
        this.session = session;
        this.fetchSize = fetchSize;
        created = session.getLastReconnect();
    }

    private void prepare(SessionRemote s, boolean createParams) {
        id = s.getNextId();
        for (int i = 0, count = 0; i < transferList.size(); i++) {
            try {
                Client transfer = transferList.get(i);
                byte[] req = new byte[65536];
                ByteBuffer bb = ByteBuffer.wrap(req);
                if (createParams) {
		    //System.out.println("iodine SESSION_PREPARE_READ_PARAMS");
                    s.traceOperation("SESSION_PREPARE_READ_PARAMS", id);
                    
                    bb.putInt(SessionRemote.SESSION_PREPARE_READ_PARAMS);
                    bb.putInt(id);
                    ByteBufferTool.putString(bb, sql);
                } else {
		    //System.out.println("iodine SESSION_PREPARE");
                    s.traceOperation("SESSION_PREPARE", id);
                    bb.putInt(SessionRemote.SESSION_PREPARE);
                    bb.putInt(id);
                    ByteBufferTool.putString(bb, sql);
                    //transfer.writeInt(SessionRemote.SESSION_PREPARE).
                    //    writeInt(id).writeString(sql);
                }
                s.requestStart();
                transfer.sendRequest(ByteBufferTool.convertBB(bb), s);
                s.done(transfer);
                isQuery = ByteBufferTool.getBoolean(s.retBB);//transfer.readBoolean();
                readonly = ByteBufferTool.getBoolean(s.retBB);//transfer.readBoolean();
                int paramCount = s.retBB.getInt();//transfer.readInt();
                if (createParams) {
                    parameters.clear();
                    for (int j = 0; j < paramCount; j++) {
                        ParameterRemote p = new ParameterRemote(j);
                        p.readMetaData(s.retBB);
                        parameters.add(p);
                    }
                }
            } catch (IOException e) {
                s.removeServer(e, i--, ++count);
            }
        }
    }

    @Override
    public boolean isQuery() {
        return isQuery;
    }

    @Override
    public ArrayList<ParameterInterface> getParameters() {
        return parameters;
    }

    private void prepareIfRequired() {
        if (session.getLastReconnect() != created) {
            // in this case we need to prepare again in every case
            id = Integer.MIN_VALUE;
        }
        session.checkClosed();
        if (id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS) {
            // object is too old - we need to prepare again
            prepare(session, false);
        }
    }

    @Override
    public ResultInterface getMetaData() {
    	throw new RuntimeException("Not implemented");
        /*synchronized (session) {
            if (!isQuery) {
                return null;
            }
            int objectId = session.getNextId();
            ResultRemote result = null;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                Transfer transfer = transferList.get(i);
                try {
		    System.out.println("iodine COMMAND_GET_META_DATA");
                    session.traceOperation("COMMAND_GET_META_DATA", id);
                    transfer.writeInt(SessionRemote.COMMAND_GET_META_DATA).
                            writeInt(id).writeInt(objectId);
                    session.done(transfer);
                    int columnCount = transfer.readInt();
                    result = new ResultRemote(session, transfer, objectId,
                            columnCount, Integer.MAX_VALUE);
                    break;
                } catch (IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.autoCommitIfCluster();
            return result;
        }*/
    }

    @Override
    public ResultInterface executeQuery(int maxRows, boolean scrollable) {
        checkParameters();
        synchronized (session) {
            int objectId = session.getNextId();
            ResultRemote result = null;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                Client transfer = transferList.get(i);
                try {
                    session.traceOperation("COMMAND_EXECUTE_QUERY", id);
                    
		    //System.out.println("iodine COMMAND_EXECUTE_QUERY time="+System.currentTimeMillis());
                    byte[] req = new byte[65536];
                    ByteBuffer bb = ByteBuffer.wrap(req);
                    bb.putInt(SessionRemote.COMMAND_EXECUTE_QUERY).
                        putInt(id).putInt(objectId).putInt(maxRows);
                    int fetch;
                    if (session.isClustered() || scrollable) {
                        fetch = Integer.MAX_VALUE;
                    } else {
                        fetch = fetchSize;
                    }
                    bb.putInt(fetch);
                    Integer version = (Integer)transfer.getProperty("clientVersion");
                    sendParameters(bb, version);
                    session.requestStart();
                    transfer.sendRequest(ByteBufferTool.convertBB(bb), session);
                    session.done(transfer);
                    int columnCount = session.retBB.getInt();//transfer.readInt();
                    if (result != null) {
                        result.close();
                        result = null;
                    }
                    result = new ResultRemote(session, transfer, objectId, columnCount, fetch);
                    if (readonly) {
                        break;
                    }
                } catch (IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.autoCommitIfCluster();
            session.readSessionState();
            return result;
        }
    }

    @Override
    public int executeUpdate() {
        checkParameters();
        synchronized (session) {
            int updateCount = 0;
            boolean autoCommit = false;
            for (int i = 0, count = 0; i < transferList.size(); i++) {
                prepareIfRequired();
                Client transfer = transferList.get(i);
                try {
		    //System.out.println("iodine COMMAND_EXECUTE_UPDATE time="+System.currentTimeMillis());
                    session.traceOperation("COMMAND_EXECUTE_UPDATE", id);
                    byte[] req = new byte[65536];
                    ByteBuffer bb = ByteBuffer.wrap(req);
                    bb.putInt(SessionRemote.COMMAND_EXECUTE_UPDATE).putInt(id);
                    Integer version = (Integer)transfer.getProperty("clientVersion");
                    sendParameters(bb, version);
                    session.requestStart();
                    transfer.sendRequest(ByteBufferTool.convertBB(bb), session);
                    session.done(transfer);
                    updateCount = session.retBB.getInt();
                    autoCommit = ByteBufferTool.getBoolean(session.retBB);
                } catch (IOException e) {
                    session.removeServer(e, i--, ++count);
                }
            }
            session.setAutoCommitFromServer(autoCommit);
            session.autoCommitIfCluster();
            session.readSessionState();
            return updateCount;
        }
    }

    private void checkParameters() {
        for (ParameterInterface p : parameters) {
            p.checkSet();
        }
    }

    private void sendParameters(ByteBuffer bb, int version) throws IOException {
        int len = parameters.size();
        bb.putInt(len);
        for (ParameterInterface p : parameters) {
            //transfer.writeValue(p.getParamValue());
            ByteBufferTool.writeValue(bb, p.getParamValue(), version, session);
        }
    }

    @Override
    public void close() {
        if (session == null || session.isClosed()) {
            return;
        }
        synchronized (session) {
            session.traceOperation("COMMAND_CLOSE", id);
            for (Client transfer : transferList) {
                try {
		    //System.out.println("iodine COMMAND_CLOSE");
                	byte [] req = new byte[8];
                	ByteBuffer bb = ByteBuffer.wrap(req);
                    bb.putInt(SessionRemote.COMMAND_CLOSE).putInt(id);
                    session.requestStart();
                    transfer.sendRequest(req, session);
                } catch (Exception e) {
                    trace.error(e, "close");
                }
            }
        }
        session = null;
        try {
            for (ParameterInterface p : parameters) {
                Value v = p.getParamValue();
                if (v != null) {
                    v.close();
                }
            }
        } catch (DbException e) {
            trace.error(e, "close");
        }
        parameters.clear();
    }

    /**
     * Cancel this current statement.
     */
    @Override
    public void cancel() {
        session.cancelStatement(id);
    }

    @Override
    public String toString() {
        return sql + Trace.formatParams(getParameters());
    }

    @Override
    public int getCommandType() {
        return UNKNOWN;
    }

}
