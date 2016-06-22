/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.h2.value.ByteBufferTool;
import org.h2.value.Transfer;

/**
 * A result set column of a remote result.
 */
public class ResultColumn {

    /**
     * The column alias.
     */
    final String alias;

    /**
     * The schema name or null.
     */
    final String schemaName;

    /**
     * The table name or null.
     */
    final String tableName;

    /**
     * The column name or null.
     */
    final String columnName;

    /**
     * The value type of this column.
     */
    final int columnType;

    /**
     * The precision.
     */
    final long precision;

    /**
     * The scale.
     */
    final int scale;

    /**
     * The expected display size.
     */
    final int displaySize;

    /**
     * True if this is an autoincrement column.
     */
    final boolean autoIncrement;

    /**
     * True if this column is nullable.
     */
    final int nullable;

    /**
     * Read an object from the given transfer object.
     *
     * @param in the object from where to read the data
     */
    ResultColumn(ByteBuffer bb) throws IOException {
        alias = ByteBufferTool.getString(bb);
        schemaName = ByteBufferTool.getString(bb);
        tableName = ByteBufferTool.getString(bb);
        columnName = ByteBufferTool.getString(bb);
        columnType = bb.getInt();
        precision = bb.getLong();
        scale = bb.getInt();
        displaySize = bb.getInt();
        autoIncrement = ByteBufferTool.getBoolean(bb);
        nullable = bb.getInt();
    }

    /**
     * Write a result column to the given output.
     *
     * @param out the object to where to write the data
     * @param result the result
     * @param i the column index
     */
    public static void writeColumn(ByteBuffer bb, ResultInterface result, int i)
            throws IOException {
        ByteBufferTool.putString(bb, result.getAlias(i));
        ByteBufferTool.putString(bb, result.getSchemaName(i));
        ByteBufferTool.putString(bb, result.getTableName(i));
        ByteBufferTool.putString(bb,result.getColumnName(i));
        bb.putInt(result.getColumnType(i));
        bb.putLong(result.getColumnPrecision(i));
        bb.putInt(result.getColumnScale(i));
        bb.putInt(result.getDisplaySize(i));
        ByteBufferTool.putBoolean(bb, result.isAutoIncrement(i));
        bb.putInt(result.getNullable(i));
    }

}
