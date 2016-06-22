/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.Command;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.Engine;
import org.h2.engine.Session;
import org.h2.engine.SessionRemote;
import org.h2.engine.SysProperties;
import org.h2.expression.Parameter;
import org.h2.expression.ParameterInterface;
import org.h2.expression.ParameterRemote;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.DbException;
import org.h2.result.ResultColumn;
import org.h2.result.ResultInterface;
import org.h2.store.LobStorageInterface;
import org.h2.util.IOUtils;
import org.h2.util.SmallLRUCache;
import org.h2.util.SmallMap;
import org.h2.util.StringUtils;
import org.h2.value.ByteBufferTool;
import org.h2.value.Transfer;
import org.h2.value.Value;
import org.h2.value.ValueLobDb;

/**
 * One server thread is opened per client connection.
 */
public class TcpServerThread {

    private final TcpServer server;
    private Session session;
    private boolean stop;
    private Thread thread;
    private Command commit;
    private final SmallMap cache =
            new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private final SmallLRUCache<Long, CachedInputStream> lobs =
            SmallLRUCache.newInstance(Math.max(
                SysProperties.SERVER_CACHED_OBJECTS,
                SysProperties.SERVER_RESULT_SET_FETCH_SIZE * 5));
    private final int threadId;
    private int clientVersion;
    private String sessionId;

    TcpServerThread(TcpServer server, int id) {
        this.server = server;
        this.threadId = id;
    }

    private void trace(String s) {
        server.trace(this + " " + s);
    }

    private boolean needFileEncryptionKey = false;
    private ConnectionInfo ci = null;
    private String originalURL = null;
    
    public byte[] init(ByteBuffer bb) {
        //try {
            trace("Connect");
            // TODO server: should support a list of allowed databases
            // and a list of allowed clients
            try {
                /*if (!server.allow(transfer.getSocket())) {
                    throw DbException.get(ErrorCode.REMOTE_CONNECTION_NOT_ALLOWED);
                }*/
                int minClientVersion = bb.getInt();
                if (minClientVersion < Constants.TCP_PROTOCOL_VERSION_6) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2,
                            "" + clientVersion, "" + Constants.TCP_PROTOCOL_VERSION_6);
                } else if (minClientVersion > Constants.TCP_PROTOCOL_VERSION_15) {
                    throw DbException.get(ErrorCode.DRIVER_VERSION_ERROR_2,
                            "" + clientVersion, "" + Constants.TCP_PROTOCOL_VERSION_15);
                }
                int maxClientVersion = bb.getInt();
                if (maxClientVersion >= Constants.TCP_PROTOCOL_VERSION_15) {
                    clientVersion = Constants.TCP_PROTOCOL_VERSION_15;
                } else {
                    clientVersion = minClientVersion;
                }
                //transfer.setVersion(clientVersion);
                String db = ByteBufferTool.getString(bb);
                originalURL = ByteBufferTool.getString(bb);
                if (db == null && originalURL == null) {
                	throw new RuntimeException("Not implemented");
                    /*String targetSessionId = ByteBufferTool.getString(bb);
                    int command = bb.getInt();
                    stop = true;
                    if (command == SessionRemote.SESSION_CANCEL_STATEMENT) {
                        // cancel a running statement
                        int statementId = bb.getInt();
                        server.cancelStatement(targetSessionId, statementId);
                    } else if (command == SessionRemote.SESSION_CHECK_KEY) {
                        // check if this is the correct server
                        db = server.checkKeyAndGetDatabaseName(targetSessionId);
                        if (!targetSessionId.equals(db)) {
                            transfer.writeInt(SessionRemote.STATUS_OK);
                        } else {
                            transfer.writeInt(SessionRemote.STATUS_ERROR);
                        }
                    }*/
                }
                String baseDir = server.getBaseDir();
                if (baseDir == null) {
                    baseDir = SysProperties.getBaseDir();
                }
                db = server.checkKeyAndGetDatabaseName(db);
                ci = new ConnectionInfo(db);
                ci.setOriginalURL(originalURL);
		System.out.println("iodine orignalURL="+originalURL);
		String userName = ByteBufferTool.getString(bb);
		System.out.println("iodine userName="+userName);
                ci.setUserName(userName);
                ci.setUserPasswordHash(ByteBufferTool.getBytes(bb));
                ci.setFilePasswordHash(ByteBufferTool.getBytes(bb));
                int len = bb.getInt();
                for (int i = 0; i < len; i++) {
                    ci.setProperty(ByteBufferTool.getString(bb), ByteBufferTool.getString(bb));
                }
                // override client's requested properties with server settings
                if (baseDir != null) {
                    ci.setBaseDir(baseDir);
                }
                if (server.getIfExists()) {
                    ci.setProperty("IFEXISTS", "TRUE");
                }
                byte[] resp = new byte[8];
                ByteBuffer respBB = ByteBuffer.wrap(resp);
                respBB.putInt(SessionRemote.STATUS_OK);
                respBB.putInt(clientVersion);
		System.out.println("iodine clientVersion = "+clientVersion+" "+Constants.TCP_PROTOCOL_VERSION_13+" "+ci.getFilePasswordHash());
                if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_13 && ci.getFilePasswordHash() != null) {
                    //if (ci.getFilePasswordHash() != null) {
                        //ci.setFileEncryptionKey(transfer.readBytes());
                    	this.needFileEncryptionKey = true;
                    //}
                }
                else{
                    session = Engine.getInstance().createSession(ci);
                    //transfer.setSession(session);
                    server.addConnection(threadId, originalURL, ci.getUserName());
                    trace("Connected");
                }
                
                //transfer.flush();
                /*if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_13) {
                    if (ci.getFilePasswordHash() != null) {
                        ci.setFileEncryptionKey(transfer.readBytes());
                    }
                }*/
                
                return resp;
            } catch (Throwable e) {
            	stop = true;
		e.printStackTrace();
                return sendError(e);
                
            }
            /*while (!stop) {
                try {
                    process();
                } catch (Throwable e) {
                    return sendError(e);
                }
            }
            trace("Disconnect");*/
       /* } catch (Throwable e) {
            server.traceError(e);
        } finally {
            close();
            return null;
        }*/
    }

    private void closeSession() {
        if (session != null) {
            RuntimeException closeError = null;
            try {
                Command rollback = session.prepareLocal("ROLLBACK");
                rollback.executeUpdate();
            } catch (RuntimeException e) {
                closeError = e;
                server.traceError(e);
            } catch (Exception e) {
                server.traceError(e);
            }
            try {
                session.close();
                server.removeConnection(threadId);
            } catch (RuntimeException e) {
                if (closeError == null) {
                    closeError = e;
                    server.traceError(e);
                }
            } catch (Exception e) {
                server.traceError(e);
            } finally {
                session = null;
            }
            if (closeError != null) {
                throw closeError;
            }
        }
    }

    /**
     * Close a connection.
     */
    void close() {
        try {
            stop = true;
            closeSession();
        } catch (Exception e) {
            server.traceError(e);
        } finally {
            //transfer.close();
            trace("Close");
            server.remove(this);
        }
    }

    private byte[] sendError(Throwable t) {
        try {
        	byte[] resp = new byte[65536];
        	ByteBuffer bb = ByteBuffer.wrap(resp);
            SQLException e = DbException.convert(t).getSQLException();
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            String trace = writer.toString();
            String message;
            String sql;
            if (e instanceof JdbcSQLException) {
                JdbcSQLException j = (JdbcSQLException) e;
                message = j.getOriginalMessage();
                sql = j.getSQL();
            } else {
                message = e.getMessage();
                sql = null;
            }
            bb.putInt(SessionRemote.STATUS_ERROR);
            ByteBufferTool.putString(bb, e.getSQLState());
            ByteBufferTool.putString(bb, message);
            ByteBufferTool.putString(bb, sql);
            bb.putInt(e.getErrorCode());
            ByteBufferTool.putString(bb, trace);
            return ByteBufferTool.convertBB(bb);
        } catch (Exception e2) {
            /*if (!transfer.isClosed()) {
                server.traceError(e2);
            }*/
            // if writing the error does not work, close the connection
            stop = true;
            return null;
        }
    }

    private void setParameters(Command command, ByteBuffer bb) throws IOException {
        int len = bb.getInt();
        ArrayList<? extends ParameterInterface> params = command.getParameters();
        for (int i = 0; i < len; i++) {
            Parameter p = (Parameter) params.get(i);
            p.setValue(ByteBufferTool.readValue(bb, this.clientVersion, this.session));
        }
    }

	public byte[] process(byte[] req) {
		try {
			ByteBuffer bb = ByteBuffer.wrap(req);
	byte[] resp = new byte[65536];
			ByteBuffer retBB = ByteBuffer.wrap(resp);
			if(this.needFileEncryptionKey){
				ci.setFileEncryptionKey(ByteBufferTool.getBytes(bb));
				session = Engine.getInstance().createSession(ci);
                		//transfer.setSession(session);
                		server.addConnection(threadId, originalURL, ci.getUserName());
                		trace("Connected");
                		return new byte[0];
			}
			int operation = bb.getInt();
			switch (operation) {
			case SessionRemote.SESSION_PREPARE_READ_PARAMS:
			case SessionRemote.SESSION_PREPARE: {
				// System.out.println("iodine2 SESSION_PREPARE");
				int id = bb.getInt();
				String sql = ByteBufferTool.getString(bb);
				int old = session.getModificationId();
				Command command = session.prepareLocal(sql);
				boolean readonly = command.isReadOnly();
				cache.addObject(id, command);
				boolean isQuery = command.isQuery();
				ArrayList<? extends ParameterInterface> params = command
						.getParameters();
				
				retBB.putInt(getState(old));
				ByteBufferTool.putBoolean(retBB, isQuery);
				ByteBufferTool.putBoolean(retBB, readonly);
				retBB.putInt(params.size());
				//transfer.writeInt(getState(old)).writeBoolean(isQuery)
				//		.writeBoolean(readonly).writeInt(params.size());
				if (operation == SessionRemote.SESSION_PREPARE_READ_PARAMS) {
					for (ParameterInterface p : params) {
						ParameterRemote.writeMetaData(retBB, p);
					}
				}
				//transfer.flush();
				//break;
				return ByteBufferTool.convertBB(retBB);
			}
			case SessionRemote.SESSION_CLOSE: {
				// System.out.println("iodine2 SESSION_CLOSE");
				stop = true;
				closeSession();
				//transfer.writeInt(SessionRemote.STATUS_OK).flush();
				retBB.putInt(SessionRemote.STATUS_OK);
				close();
				return ByteBufferTool.convertBB(retBB);
			}
			case SessionRemote.COMMAND_COMMIT: {
				throw new RuntimeException("Not implemented");
				/*System.out.println("iodine COMMAND_COMMIT");
				if (commit == null) {
					commit = session.prepareLocal("COMMIT");
				}
				int old = session.getModificationId();
				commit.executeUpdate();
				transfer.writeInt(getState(old)).flush();
				break;*/
			}
			case SessionRemote.COMMAND_GET_META_DATA: {
				throw new RuntimeException("Not implemented");
				/*System.out.println("iodine COMMAND_GET_META_DATA");
				int id = transfer.readInt();
				int objectId = transfer.readInt();
				Command command = (Command) cache.getObject(id, false);
				ResultInterface result = command.getMetaData();
				cache.addObject(objectId, result);
				int columnCount = result.getVisibleColumnCount();
				transfer.writeInt(SessionRemote.STATUS_OK)
						.writeInt(columnCount).writeInt(0);
				for (int i = 0; i < columnCount; i++) {
					ByteBuffer bb = null;
					ResultColumn.writeColumn(bb, result, i);
				}
				transfer.flush();
				break;*/
			}
			case SessionRemote.COMMAND_EXECUTE_QUERY: {
				// System.out.println("iodine2 COMMAND_EXECUTE_QUERY");
				long start = System.currentTimeMillis();
				int id = bb.getInt();
				int objectId = bb.getInt();
				int maxRows = bb.getInt();
				int fetchSize = bb.getInt();
				Command command = (Command) cache.getObject(id, false);
				setParameters(command, bb);
				int old = session.getModificationId();
				ResultInterface result;
				synchronized (session) {
					result = command.executeQuery(maxRows, false);
				}
				cache.addObject(objectId, result);
				int columnCount = result.getVisibleColumnCount();
				int state = getState(old);
				retBB.putInt(state).putInt(columnCount);
				//transfer.writeInt(state).writeInt(columnCount);
				int rowCount = result.getRowCount();
				retBB.putInt(rowCount);
				//transfer.writeInt(rowCount);
				for (int i = 0; i < columnCount; i++) {
					ResultColumn.writeColumn(retBB, result, i);
				}
				int fetch = Math.min(rowCount, fetchSize);
				for (int i = 0; i < fetch; i++) {
					sendRow(result, retBB);
				}
				//transfer.flush();
				//break;
				//if(System.currentTimeMillis()-start>1000)
				//    System.out.println("query time = "+(System.currentTimeMillis()-start));
				return ByteBufferTool.convertBB(retBB);
			}
			case SessionRemote.COMMAND_EXECUTE_UPDATE: {
				// System.out.println("iodine2 COMMAND_EXECUTE_UPDATE");
				 long start = System.currentTimeMillis();
				int id = bb.getInt();
				Command command = (Command) cache.getObject(id, false);
				setParameters(command, bb);
				int old = session.getModificationId();
				int updateCount;
				synchronized (session) {
					updateCount = command.executeUpdate();
				}
				int status;
				if (session.isClosed()) {
					status = SessionRemote.STATUS_CLOSED;
				} else {
					status = getState(old);
				}
				retBB.putInt(status).putInt(updateCount);
				ByteBufferTool.putBoolean(retBB, session.getAutoCommit());
				//transfer.writeInt(status).writeInt(updateCount)
				//		.writeBoolean(session.getAutoCommit());
				//transfer.flush();
				//break;
				//if(System.currentTimeMillis()-start>1000)
                                //    System.out.println("update time = "+(System.currentTimeMillis()-start));
				return ByteBufferTool.convertBB(retBB);
			}
			case SessionRemote.COMMAND_CLOSE: {
				// System.out.println("iodine2 COMMAND_CLOSE");
				int id = bb.getInt();
				Command command = (Command) cache.getObject(id, true);
				if (command != null) {
					command.close();
					cache.freeObject(id);
				}
				return new byte[0];
			}
			case SessionRemote.RESULT_FETCH_ROWS: {
				throw new RuntimeException("Not implemented");
				/*System.out.println("iodine RESULT_FETCH_ROWS");
				
				int id = transfer.readInt();
				int count = transfer.readInt();
				ResultInterface result = (ResultInterface) cache.getObject(id,
						false);
				transfer.writeInt(SessionRemote.STATUS_OK);
				for (int i = 0; i < count; i++) {
					sendRow(result);
				}
				transfer.flush();
				break;*/
			}
			case SessionRemote.RESULT_RESET: {
				throw new RuntimeException("Not implemented");
				/*System.out.println("iodine RESULT_RESET");
				int id = transfer.readInt();
				ResultInterface result = (ResultInterface) cache.getObject(id,
						false);
				result.reset();
				break;*/
			}
			case SessionRemote.RESULT_CLOSE: {
				// System.out.println("iodine2 RESULT_CLOSE");
				int id = bb.getInt();
				ResultInterface result = (ResultInterface) cache.getObject(id,
						true);
				if (result != null) {
					result.close();
					cache.freeObject(id);
				}
				return new byte[0];
			}
			case SessionRemote.CHANGE_ID: {
				throw new RuntimeException("Not implemented");
				/*System.out.println("iodine CHANGE_ID");
				int oldId = transfer.readInt();
				int newId = transfer.readInt();
				Object obj = cache.getObject(oldId, false);
				cache.freeObject(oldId);
				cache.addObject(newId, obj);
				break;*/
			}
			case SessionRemote.SESSION_SET_ID: {
				// System.out.println("iodine2 SESSION_SET_ID");
				sessionId = ByteBufferTool.getString(bb);//transfer.readString();
				retBB.putInt(SessionRemote.STATUS_OK);
				ByteBufferTool.putBoolean(retBB, session.getAutoCommit());
				//transfer.writeBoolean(session.getAutoCommit());
				//transfer.flush();
				//break;
				return ByteBufferTool.convertBB(retBB);
			}
			case SessionRemote.SESSION_SET_AUTOCOMMIT: {
				// System.out.println("iodine2 SESSION_SET_AUTOCOMMIT");
				boolean autoCommit = ByteBufferTool.getBoolean(bb);//transfer.readBoolean();
				session.setAutoCommit(autoCommit);
				retBB.putInt(SessionRemote.STATUS_OK);
				return ByteBufferTool.convertBB(retBB);
				//transfer.writeInt(SessionRemote.STATUS_OK).flush();
				//break;
			}
			case SessionRemote.SESSION_HAS_PENDING_TRANSACTION: {
				 // System.out.println("iodine2 SESSION_HAS_PENDING_TRANSACTION");
				retBB.putInt(SessionRemote.STATUS_OK)
						.putInt(session.hasPendingTransaction() ? 1 : 0);
						//.flush();
				return ByteBufferTool.convertBB(retBB);
				//break;
			}
			case SessionRemote.LOB_READ: {
				throw new RuntimeException("Not implemented");
				/*System.out.println("iodine LOB_READ");
				long lobId = transfer.readLong();
				byte[] hmac;
				CachedInputStream in;
				boolean verifyMac;
				if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_11) {
					if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_12) {
						hmac = transfer.readBytes();
						verifyMac = true;
					} else {
						hmac = null;
						verifyMac = false;
					}
					in = lobs.get(lobId);
					if (in == null && verifyMac) {
						in = new CachedInputStream(null);
						lobs.put(lobId, in);
					}
				} else {
					verifyMac = false;
					hmac = null;
					in = lobs.get(lobId);
				}
				long offset = transfer.readLong();
				int length = transfer.readInt();
				if (verifyMac) {
					transfer.verifyLobMac(hmac, lobId);
				}
				if (in == null) {
					throw DbException.get(ErrorCode.OBJECT_CLOSED);
				}
				if (in.getPos() != offset) {
					LobStorageInterface lobStorage = session.getDataHandler()
							.getLobStorage();
					// only the lob id is used
					ValueLobDb lob = ValueLobDb.create(Value.BLOB, null, -1,
							lobId, hmac, -1);
					InputStream lobIn = lobStorage
							.getInputStream(lob, hmac, -1);
					in = new CachedInputStream(lobIn);
					lobs.put(lobId, in);
					lobIn.skip(offset);
				}
				// limit the buffer size
				length = Math.min(16 * Constants.IO_BUFFER_SIZE, length);
				byte[] buff = new byte[length];
				length = IOUtils.readFully(in, buff, length);
				transfer.writeInt(SessionRemote.STATUS_OK);
				transfer.writeInt(length);
				transfer.writeBytes(buff, 0, length);
				transfer.flush();
				break;*/
			}
			default:
				trace("Unknown operation: " + operation);
				closeSession();
				close();
				return new byte[0];
			}
		} catch (IOException e) {
			return sendError(e);
		}
	}

    private int getState(int oldModificationId) {
        if (session.getModificationId() == oldModificationId) {
            return SessionRemote.STATUS_OK;
        }
        return SessionRemote.STATUS_OK_STATE_CHANGED;
    }

    private void sendRow(ResultInterface result, ByteBuffer bb) throws IOException {
        if (result.next()) {
        	ByteBufferTool.putBoolean(bb, true);
            //bb.writeBoolean(true);
            Value[] v = result.currentRow();
            for (int i = 0; i < result.getVisibleColumnCount(); i++) {
                if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_12) {
                    ByteBufferTool.writeValue(bb, v[i], clientVersion, session);
                } else {
                    writeValue(bb, v[i]);
                }
            }
        } else {
        	ByteBufferTool.putBoolean(bb, false);
            //transfer.writeBoolean(false);
        }
    }

    private void writeValue(ByteBuffer bb, Value v) throws IOException {
        if (v.getType() == Value.CLOB || v.getType() == Value.BLOB) {
            if (v instanceof ValueLobDb) {
                ValueLobDb lob = (ValueLobDb) v;
                if (lob.isStored()) {
                    long id = lob.getLobId();
                    lobs.put(id, new CachedInputStream(null));
                }
            }
        }
        ByteBufferTool.writeValue(bb, v, this.clientVersion, session);
    }

    void setThread(Thread thread) {
        this.thread = thread;
    }

    Thread getThread() {
        return thread;
    }

    /**
     * Cancel a running statement.
     *
     * @param targetSessionId the session id
     * @param statementId the statement to cancel
     */
    void cancelStatement(String targetSessionId, int statementId) {
        if (StringUtils.equals(targetSessionId, this.sessionId)) {
            Command cmd = (Command) cache.getObject(statementId, false);
            cmd.cancel();
        }
    }

    /**
     * An input stream with a position.
     */
    static class CachedInputStream extends FilterInputStream {

        private static final ByteArrayInputStream DUMMY =
                new ByteArrayInputStream(new byte[0]);
        private long pos;

        CachedInputStream(InputStream in) {
            super(in == null ? DUMMY : in);
            if (in == null) {
                pos = -1;
            }
        }

        @Override
        public int read(byte[] buff, int off, int len) throws IOException {
            len = super.read(buff, off, len);
            if (len > 0) {
                pos += len;
            }
            return len;
        }

        @Override
        public int read() throws IOException {
            int x = in.read();
            if (x >= 0) {
                pos++;
            }
            return x;
        }

        @Override
        public long skip(long n) throws IOException {
            n = super.skip(n);
            if (n > 0) {
                pos += n;
            }
            return n;
        }

        public long getPos() {
            return pos;
        }

    }

}
