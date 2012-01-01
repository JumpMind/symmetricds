package org.jumpmind.db.platform.postgresql;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialBlob;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.log.Log;

/*
 * The platform implementation for PostgresSql.
 */
public class PostgreSqlPlatform extends AbstractJdbcDatabasePlatform {
    /* Database name of this platform. */
    public static final String DATABASENAME = "PostgreSql";
    /* The standard PostgreSQL jdbc driver. */
    public static final String JDBC_DRIVER = "org.postgresql.Driver";
    /* The subprotocol used by the standard PostgreSQL driver. */
    public static final String JDBC_SUBPROTOCOL = "postgresql";

    /*
     * Creates a new platform instance.
     */
    public PostgreSqlPlatform(DataSource dataSource, DatabasePlatformSettings settings, Log log) {
        super(dataSource, settings, log);
        
        // Query timeout needs to be zero for postrgres because the jdbc driver does
        // not support a timeout setting of of other than zero.
        settings.setQueryTimeout(0);

        // this is the default length though it might be changed when building
        // PostgreSQL
        // in file src/include/postgres_ext.h
        info.setMaxIdentifierLength(31);

        info.addNativeTypeMapping(Types.ARRAY, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BINARY, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BIT, "BOOLEAN");
        info.addNativeTypeMapping(Types.BLOB, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.CLOB, "TEXT", Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.DECIMAL, "NUMERIC", Types.NUMERIC);
        info.addNativeTypeMapping(Types.DISTINCT, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        info.addNativeTypeMapping(Types.FLOAT, "DOUBLE PRECISION", Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "BYTEA");
        info.addNativeTypeMapping(Types.LONGVARCHAR, "TEXT", Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.NULL, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.OTHER, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.REF, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.STRUCT, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping(Types.VARBINARY, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping("BOOLEAN", "BOOLEAN", "BIT");
        info.addNativeTypeMapping("DATALINK", "BYTEA", "LONGVARBINARY");

        info.setDefaultSize(Types.CHAR, 254);
        info.setDefaultSize(Types.VARCHAR, 254);

        // no support for specifying the size for these types (because they are
        // mapped
        // to BYTEA which back-maps to BLOB)
        info.setHasSize(Types.BINARY, false);
        info.setHasSize(Types.VARBINARY, false);

        setDelimitedIdentifierModeOn(true);
        info.setNonBlankCharColumnSpacePadded(true);
        info.setBlankCharColumnSpacePadded(true);
        info.setCharColumnSpaceTrimmed(false);
        info.setEmptyStringNulled(false);

        primaryKeyViolationSqlStates = new String[] {"23505"};

        ddlReader = new PostgreSqlDdlReader(log, this);
        ddlBuilder = new PostgreSqlBuilder(log, this);
    }

    public String getName() {
        return DATABASENAME;
    }
    
    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject("select current_schema()", String.class);
        }
        return defaultSchema;
    }
    
    public String getDefaultCatalog() {
        return null;
    }
    

    @Override
    protected Array createArray(Column column, final String value) {
        if (StringUtils.isNotBlank(value)) {

            String jdbcTypeName = column.getJdbcTypeName();
            if (jdbcTypeName.startsWith("_")) {
                jdbcTypeName = jdbcTypeName.substring(1);
            }
            int jdbcBaseType = Types.VARCHAR;
            if (jdbcTypeName.toLowerCase().contains("int")) {
                jdbcBaseType = Types.INTEGER;
            }
                        
            final String baseTypeName = jdbcTypeName;
            final int baseType = jdbcBaseType;
            return new Array() {
                public String getBaseTypeName() {
                    return baseTypeName;
                }

                public void free() throws SQLException {
                }

                public int getBaseType() {
                    return baseType;
                }

                public Object getArray() {
                    return null;
                }

                public Object getArray(Map<String, Class<?>> map) {
                    return null;
                }

                public Object getArray(long index, int count) {
                    return null;
                }

                public Object getArray(long index, int count, Map<String, Class<?>> map) {
                    return null;
                }

                public ResultSet getResultSet() {
                    return null;
                }

                public ResultSet getResultSet(Map<String, Class<?>> map) {
                    return null;
                }

                public ResultSet getResultSet(long index, int count) {
                    return null;
                }

                public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) {
                    return null;
                }

                public String toString() {
                    return value;
                }
            };
        } else {
            return null;
        }
    }
    
    @Override
    protected String cleanTextForTextBasedColumns(String text) {
        return text.replace("\0", "");
    }
    
    
    @Override
    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
        Column[] orderedMetaData) {

        Object[] objectValues = super.getObjectValues(encoding, values, orderedMetaData);
        for (int i = 0; i < orderedMetaData.length; i++) {
            if (orderedMetaData[i] != null && orderedMetaData[i].getTypeCode() == Types.BLOB
                    && objectValues[i] != null) {
                try {
                    objectValues[i] = new SerialBlob((byte[]) objectValues[i]);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }                
            }
        }
        return objectValues;
    }
    
    @Override
    public DmlStatement createDmlStatement(DmlType dmlType, String catalogName, String schemaName,
            String tableName, Column[] keys, Column[] columns) {
        return new PostgresDmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                getPlatformInfo().isDateOverridesToTimestamp(),
                getPlatformInfo().getIdentifierQuoteString());
    }
    
}
