/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import glassPaxos.client.Client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.h2.engine.SessionRemote;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.util.New;
import org.h2.value.ByteBufferTool;
import org.h2.value.Transfer;
import org.h2.value.Value;

/**
 * The client side part of a result set that is kept on the server.
 * In many cases, the complete data is kept on the client side,
 * but for large results only a subset is in-memory.
 */
public class ResultRemote implements ResultInterface {

    private int fetchSize;
    private SessionRemote session;
    private Client transfer;
    private int id;
    private final ResultColumn[] columns;
    private Value[] currentRow;
    private final int rowCount;
    private int rowId, rowOffset;
    private ArrayList<Value[]> result;
    private final Trace trace;

    public ResultRemote(SessionRemote session, Client transfer, int id,
            int columnCount, int fetchSize) throws IOException {
        this.session = session;
        trace = session.getTrace();
        this.transfer = transfer;
        this.id = id;
        this.columns = new ResultColumn[columnCount];
        rowCount = session.retBB.getInt();
        for (int i = 0; i < columnCount; i++) {
            columns[i] = new ResultColumn(session.retBB);
        }
        rowId = -1;
        result = New.arrayList();
        this.fetchSize = fetchSize;
        fetchRows(false);
    }

    @Override
    public String getAlias(int i) {
        return columns[i].alias;
    }

    @Override
    public String getSchemaName(int i) {
        return columns[i].schemaName;
    }

    @Override
    public String getTableName(int i) {
        return columns[i].tableName;
    }

    @Override
    public String getColumnName(int i) {
        return columns[i].columnName;
    }

    @Override
    public int getColumnType(int i) {
        return columns[i].columnType;
    }

    @Override
    public long getColumnPrecision(int i) {
        return columns[i].precision;
    }

    @Override
    public int getColumnScale(int i) {
        return columns[i].scale;
    }

    @Override
    public int getDisplaySize(int i) {
        return columns[i].displaySize;
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return columns[i].autoIncrement;
    }

    @Override
    public int getNullable(int i) {
        return columns[i].nullable;
    }

    @Override
    public void reset() {
    	throw new RuntimeException("Not implemented");
        /*rowId = -1;
        currentRow = null;
        if (session == null) {
            return;
        }
        synchronized (session) {
            session.checkClosed();
            try {
                session.traceOperation("RESULT_RESET", id);
                transfer.writeInt(SessionRemote.RESULT_RESET).writeInt(id).flush();
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        }*/
    }

    @Override
    public Value[] currentRow() {
        return currentRow;
    }

    @Override
    public boolean next() {
        if (rowId < rowCount) {
            rowId++;
            remapIfOld();
            if (rowId < rowCount) {
                if (rowId - rowOffset >= result.size()) {
                    fetchRows(true);
                }
                currentRow = result.get(rowId - rowOffset);
                return true;
            }
            currentRow = null;
        }
        return false;
    }

    @Override
    public int getRowId() {
        return rowId;
    }

    @Override
    public int getVisibleColumnCount() {
        return columns.length;
    }

    @Override
    public int getRowCount() {
        return rowCount;
    }

    private void sendClose() {
        if (session == null) {
            return;
        }
        // TODO result sets: no reset possible for larger remote result sets
        try {
            synchronized (session) {
		//System.out.println("iodine RESULT_CLOSE");
                session.traceOperation("RESULT_CLOSE", id);
                byte[] req = new byte[8];
                ByteBuffer bb = ByteBuffer.wrap(req);
                bb.putInt(SessionRemote.RESULT_CLOSE).putInt(id);
                session.requestStart();
                transfer.sendRequest(req, session);
                //transfer.writeInt(SessionRemote.RESULT_CLOSE).writeInt(id);
            }
        } catch (Exception e) {
            trace.error(e, "close");
        } finally {
            transfer = null;
            session = null;
        }
    }

    @Override
    public void close() {
        result = null;
        sendClose();
    }

    private void remapIfOld() {
        if (session == null) {
            return;
        }
    	throw new RuntimeException("not implemented");
        /*try {
            if (id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS / 2) {
                // object is too old - we need to map it to a new id
                int newId = session.getNextId();
                session.traceOperation("CHANGE_ID", id);
                transfer.writeInt(SessionRemote.CHANGE_ID).writeInt(id).writeInt(newId);
                id = newId;
                // TODO remote result set: very old result sets may be
                // already removed on the server (theoretically) - how to
                // solve this?
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }*/
    }

    private void fetchRows(boolean sendFetch) {
    	
        synchronized (session) {
            session.checkClosed();
            try {
                rowOffset += result.size();
                result.clear();
                int fetch = Math.min(fetchSize, rowCount - rowOffset);
                if (sendFetch) {
                	throw new RuntimeException("Not implemented");
                    /*session.traceOperation("RESULT_FETCH_ROWS", id);
                    transfer.writeInt(SessionRemote.RESULT_FETCH_ROWS).
                            writeInt(id).writeInt(fetch);
                    session.done(transfer);*/
                }
                for (int r = 0; r < fetch; r++) {
                    boolean row = ByteBufferTool.getBoolean(session.retBB);//transfer.readBoolean();
                    if (!row) {
                        break;
                    }
                    int len = columns.length;
                    Value[] values = new Value[len];
                    for (int i = 0; i < len; i++) {
                        Value v = ByteBufferTool.readValue(session.retBB, session.clientVersion, session);
                        values[i] = v;
                    }
                    result.add(values);
                }
                if (rowOffset + result.size() >= rowCount) {
                    sendClose();
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        }
    }

    @Override
    public String toString() {
        return "columns: " + columns.length + " rows: " + rowCount + " pos: " + rowId;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public boolean needToClose() {
        return true;
    }

}
