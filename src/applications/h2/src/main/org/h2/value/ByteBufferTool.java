package org.h2.value;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.engine.SessionInterface;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.security.SHA256;
import org.h2.store.Data;
import org.h2.store.DataReader;
import org.h2.tools.SimpleResultSet;
import org.h2.util.DateTimeUtils;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.Utils;

public class ByteBufferTool {
	public static void putString(ByteBuffer bb, String str){
		if(str == null){
			bb.putInt(-1);
		}
		else{
			byte[] tmp = str.getBytes();
			bb.putInt(tmp.length);
			bb.put(tmp);
		}
	}
	
	public static String getString(ByteBuffer bb){
		int size = bb.getInt();
		if(size == -1)
			return null;
		else{
			byte[] tmp = new byte[size];
			bb.get(tmp);
			return new String(tmp);
		}
	}
	
	public static byte[] convertBB(ByteBuffer bb){
		byte[] ret = new byte[bb.position()];
		bb.rewind();
		bb.get(ret);
		return ret;
	}
	
	public static void putBoolean(ByteBuffer bb, boolean val){
		bb.put((byte) (val ? 1 : 0));
	}
	
	public static boolean getBoolean(ByteBuffer bb){
		return bb.get() == 1;
	}
	
	public static void putBytes(ByteBuffer bb, byte[] data) throws IOException {
        if (data == null) {
            bb.putInt(-1);
        } else {
            bb.putInt(data.length);
            bb.put(data);
        }
	}
	
	public static byte[] getBytes(ByteBuffer bb) throws IOException {
        int len = bb.getInt();
        if (len == -1) {
            return null;
        }
        byte[] b = DataUtils.newBytes(len);
        bb.get(b);
        return b;
    }
	
	public static void writeValue(ByteBuffer bb, Value v, int version, SessionInterface session) throws IOException {
        int type = v.getType();
        bb.putInt(type);
        switch (type) {
        case Value.NULL:
            break;
        case Value.BYTES:
        case Value.JAVA_OBJECT:
            bb.put(v.getBytesNoCopy());
            break;
        case Value.UUID: {
            ValueUuid uuid = (ValueUuid) v;
            bb.putLong(uuid.getHigh());
            bb.putLong(uuid.getLow());
            break;
        }
        case Value.BOOLEAN:
            putBoolean(bb, v.getBoolean().booleanValue());
            break;
        case Value.BYTE:
            bb.put(v.getByte());
            break;
        case Value.TIME:
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                bb.putLong(((ValueTime) v).getNanos());
            } else if (version >= Constants.TCP_PROTOCOL_VERSION_7) {
                bb.putLong(DateTimeUtils.getTimeLocalWithoutDst(v.getTime()));
            } else {
                bb.putLong(v.getTime().getTime());
            }
            break;
        case Value.DATE:
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                bb.putLong(((ValueDate) v).getDateValue());
            } else if (version >= Constants.TCP_PROTOCOL_VERSION_7) {
                bb.putLong(DateTimeUtils.getTimeLocalWithoutDst(v.getDate()));
            } else {
                bb.putLong(v.getDate().getTime());
            }
            break;
        case Value.TIMESTAMP: {
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                ValueTimestamp ts = (ValueTimestamp) v;
                bb.putLong(ts.getDateValue());
                bb.putLong(ts.getTimeNanos());
            } else if (version >= Constants.TCP_PROTOCOL_VERSION_7) {
                Timestamp ts = v.getTimestamp();
                bb.putLong(DateTimeUtils.getTimeLocalWithoutDst(ts));
                bb.putInt(ts.getNanos() % 1000000);
            } else {
                Timestamp ts = v.getTimestamp();
                bb.putLong(ts.getTime());
                bb.putInt(ts.getNanos() % 1000000);
            }
            break;
        }
        case Value.DECIMAL:
            putString(bb, v.getString());
            break;
        case Value.DOUBLE:
            bb.putDouble(v.getDouble());
            break;
        case Value.FLOAT:
            bb.putFloat(v.getFloat());
            break;
        case Value.INT:
            bb.putInt(v.getInt());
            break;
        case Value.LONG:
            bb.putLong(v.getLong());
            break;
        case Value.SHORT:
            bb.putInt(v.getShort());
            break;
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            putString(bb, v.getString());
            break;
        case Value.BLOB: {
            if (version >= Constants.TCP_PROTOCOL_VERSION_11) {
                if (v instanceof ValueLobDb) {
                    ValueLobDb lob = (ValueLobDb) v;
                    if (lob.isStored()) {
                        bb.putLong(-1);
                        bb.putInt(lob.getTableId());
                        bb.putLong(lob.getLobId());
                        if (version >= Constants.TCP_PROTOCOL_VERSION_12) {
                            bb.put(calculateLobMac(lob.getLobId()));
                        }
                        bb.putLong(lob.getPrecision());
                        break;
                    }
                }
            }
            long length = v.getPrecision();
            if (length < 0) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "length=" + length);
            }
            bb.putLong(length);
            long written = copy(v.getInputStream(), bb, Long.MAX_VALUE);
            if (written != length) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "length:" + length + " written:" + written);
            }
            bb.putInt(LOB_MAGIC);
            break;
        }
        case Value.CLOB: {
            if (version >= Constants.TCP_PROTOCOL_VERSION_11) {
                if (v instanceof ValueLobDb) {
                    ValueLobDb lob = (ValueLobDb) v;
                    if (lob.isStored()) {
                        bb.putLong(-1);
                        bb.putInt(lob.getTableId());
                        bb.putLong(lob.getLobId());
                        if (version >= Constants.TCP_PROTOCOL_VERSION_12) {
                            bb.put(calculateLobMac(lob.getLobId()));
                        }
                        bb.putLong(lob.getPrecision());
                        break;
                    }
                }
            }
            long length = v.getPrecision();
            if (length < 0) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "length=" + length);
            }
            bb.putLong(length);
            Reader reader = v.getReader();
            Data.copyString(reader, bb);
            bb.putInt(LOB_MAGIC);
            break;
        }
        case Value.ARRAY: {
            ValueArray va = (ValueArray) v;
            Value[] list = va.getList();
            int len = list.length;
            Class<?> componentType = va.getComponentType();
            if (componentType == Object.class) {
                bb.putInt(len);
            } else {
                bb.putInt(-(len + 1));
                putString(bb, componentType.getName());
            }
            for (Value value : list) {
                writeValue(bb, value, version, session);
            }
            break;
        }
        case Value.RESULT_SET: {
            try {
                ResultSet rs = ((ValueResultSet) v).getResultSet();
                rs.beforeFirst();
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                bb.putInt(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    putString(bb, meta.getColumnName(i + 1));
                    bb.putInt(meta.getColumnType(i + 1));
                    bb.putInt(meta.getPrecision(i + 1));
                    bb.putInt(meta.getScale(i + 1));
                }
                while (rs.next()) {
                    putBoolean(bb, true);
                    for (int i = 0; i < columnCount; i++) {
                        int t = DataType.getValueTypeFromResultSet(meta, i + 1);
                        Value val = DataType.readValue(session, rs, i + 1, t);
                        writeValue(bb, val, version, session);
                    }
                }
                putBoolean(bb, false);
                rs.beforeFirst();
            } catch (SQLException e) {
                throw DbException.convertToIOException(e);
            }
            break;
        }
        case Value.GEOMETRY:
            if (version >= Constants.TCP_PROTOCOL_VERSION_14) {
                bb.put(v.getBytesNoCopy());
            } else {
                putString(bb, v.getString());
            }
            break;
        default:
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "type=" + type);
        }
    }

    /**
     * Read a value.
     *
     * @return the value
     */
    public static Value readValue(ByteBuffer bb, int version, SessionInterface session) throws IOException {
        int type = bb.getInt();
        switch(type) {
        case Value.NULL:
            return ValueNull.INSTANCE;
        case Value.BYTES:
            return ValueBytes.getNoCopy(getBytes(bb));
        case Value.UUID:
            return ValueUuid.get(bb.getLong(), bb.getLong());
        case Value.JAVA_OBJECT:
            return ValueJavaObject.getNoCopy(null, getBytes(bb), session.getDataHandler());
        case Value.BOOLEAN:
            return ValueBoolean.get(getBoolean(bb));
        case Value.BYTE:
            return ValueByte.get(bb.get());
        case Value.DATE:
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                return ValueDate.fromDateValue(bb.getLong());
            } else if (version >= Constants.TCP_PROTOCOL_VERSION_7) {
                return ValueDate.fromMillis(DateTimeUtils.getTimeUTCWithoutDst(bb.getLong()));
            }
            return ValueDate.fromMillis(bb.getLong());
        case Value.TIME:
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                return ValueTime.fromNanos(bb.getLong());
            } else if (version >= Constants.TCP_PROTOCOL_VERSION_7) {
                return ValueTime.fromMillis(DateTimeUtils.getTimeUTCWithoutDst(bb.getLong()));
            }
            return ValueTime.fromMillis(bb.getLong());
        case Value.TIMESTAMP: {
            if (version >= Constants.TCP_PROTOCOL_VERSION_9) {
                return ValueTimestamp.fromDateValueAndNanos(
                        bb.getLong(), bb.getLong());
            } else if (version >= Constants.TCP_PROTOCOL_VERSION_7) {
                return ValueTimestamp.fromMillisNanos(
                        DateTimeUtils.getTimeUTCWithoutDst(bb.getLong()),
                        bb.getInt() % 1000000);
            }
            return ValueTimestamp.fromMillisNanos(bb.getLong(),
                    bb.getInt() % 1000000);
        }
        case Value.DECIMAL:
            return ValueDecimal.get(new BigDecimal(getString(bb)));
        case Value.DOUBLE:
            return ValueDouble.get(bb.getDouble());
        case Value.FLOAT:
            return ValueFloat.get(bb.getFloat());
        case Value.INT:
            return ValueInt.get(bb.getInt());
        case Value.LONG:
            return ValueLong.get(bb.getLong());
        case Value.SHORT:
            return ValueShort.get((short) bb.getInt());
        case Value.STRING:
            return ValueString.get(getString(bb));
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(getString(bb));
        case Value.STRING_FIXED:
            return ValueStringFixed.get(getString(bb));
        case Value.BLOB: {
        	throw new RuntimeException("not implemented");
            /*long length = bb.getLong();
            if (version >= Constants.TCP_PROTOCOL_VERSION_11) {
                if (length == -1) {
                    int tableId = bb.getInt();
                    long id = bb.getLong();
                    byte[] hmac;
                    if (version >= Constants.TCP_PROTOCOL_VERSION_12) {
                        hmac = getBytes(bb);
                    } else {
                        hmac = null;
                    }
                    long precision = bb.getLong();
                    return ValueLobDb.create(
                            Value.BLOB, session.getDataHandler(), tableId, id, hmac, precision);
                }
                int len = (int) length;
                byte[] small = new byte[len];
                //IOUtils.readFully(in, small, len);
                bb.get(small);
                int magic = bb.getInt();
                if (magic != LOB_MAGIC) {
                    throw DbException.get(
                            ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
                }
                return ValueLobDb.createSmallLob(Value.BLOB, small, length);
            }
            Value v = session.getDataHandler().getLobStorage().createBlob(in, length);
            int magic = bb.getInt();
            if (magic != LOB_MAGIC) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
            }
            return v;*/
        }
        case Value.CLOB: {
        	throw new RuntimeException("not implemented");
            /*long length = bb.getLong();
            if (version >= Constants.TCP_PROTOCOL_VERSION_11) {
                if (length == -1) {
                    int tableId = bb.getInt();
                    long id = bb.getLong();
                    byte[] hmac;
                    if (version >= Constants.TCP_PROTOCOL_VERSION_12) {
                        hmac = getBytes(bb);
                    } else {
                        hmac = null;
                    }
                    long precision = bb.getLong();
                    return ValueLobDb.create(
                            Value.CLOB, session.getDataHandler(), tableId, id, hmac, precision);
                }
                DataReader reader = new DataReader(in);
                int len = (int) length;
                char[] buff = new char[len];
                IOUtils.readFully(reader, buff, len);
                int magic = bb.getInt();
                if (magic != LOB_MAGIC) {
                    throw DbException.get(
                            ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
                }
                byte[] small = new String(buff).getBytes(Constants.UTF8);
                return ValueLobDb.createSmallLob(Value.CLOB, small, length);
            }
            Value v = session.getDataHandler().getLobStorage().
                    createClob(new DataReader(in), length);
            int magic = readInt();
            if (magic != LOB_MAGIC) {
                throw DbException.get(
                        ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
            }
            return v;*/
        }
        case Value.ARRAY: {
            int len = bb.getInt();
            Class<?> componentType = Object.class;
            if (len < 0) {
                len = -(len + 1);
                componentType = JdbcUtils.loadUserClass(getString(bb));
            }
            Value[] list = new Value[len];
            for (int i = 0; i < len; i++) {
                list[i] = readValue(bb, version, session);
            }
            return ValueArray.get(componentType, list);
        }
        case Value.RESULT_SET: {
            SimpleResultSet rs = new SimpleResultSet();
            rs.setAutoClose(false);
            int columns = bb.getInt();
            for (int i = 0; i < columns; i++) {
                rs.addColumn(getString(bb), bb.getInt(), bb.getInt(), bb.getInt());
            }
            while (true) {
                if (!getBoolean(bb)) {
                    break;
                }
                Object[] o = new Object[columns];
                for (int i = 0; i < columns; i++) {
                    o[i] = readValue(bb, version, session).getObject();
                }
                rs.addRow(o);
            }
            return ValueResultSet.get(rs);
        }
        case Value.GEOMETRY:
            if (version >= Constants.TCP_PROTOCOL_VERSION_14) {
                return ValueGeometry.get(getBytes(bb));
            }
            return ValueGeometry.get(getString(bb));
        default:
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "type=" + type);
        }
    }
    
    private static byte[] lobMacSalt;
    private static final int LOB_MAGIC = 0x1234;
    private static final int LOB_MAC_SALT_LENGTH = 16;
    private static byte[] calculateLobMac(long lobId) {
        if (lobMacSalt == null) {
            lobMacSalt = MathUtils.secureRandomBytes(LOB_MAC_SALT_LENGTH);
        }
        byte[] data = new byte[8];
        Utils.writeLong(data, 0, lobId);
        byte[] hmacData = SHA256.getHashWithSalt(data, lobMacSalt);
        return hmacData;
    }
    
    public static long copy(InputStream in, ByteBuffer out, long length)
            throws IOException {
        try {
            long copied = 0;
            int len = (int) Math.min(length, Constants.IO_BUFFER_SIZE);
            byte[] buffer = new byte[len];
            while (length > 0) {
                len = in.read(buffer, 0, len);
                if (len < 0) {
                    break;
                }
                if (out != null) {
                    out.put(buffer, 0, len);
                }
                copied += len;
                length -= len;
                len = (int) Math.min(length, Constants.IO_BUFFER_SIZE);
            }
            return copied;
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        }
    }
}
