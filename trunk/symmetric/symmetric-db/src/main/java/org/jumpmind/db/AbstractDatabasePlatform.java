package org.jumpmind.db;

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

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.JdbcModelReader;
import org.jumpmind.db.platform.SqlBuilder;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.util.Log;
import org.jumpmind.util.LogFactory;

/*
 * Base class for platform implementations.
 */
public abstract class AbstractDatabasePlatform implements IDatabasePlatform {

    /* The default name for models read from the database, if no name as given. */
    protected static final String MODEL_DEFAULT_NAME = "default";

    /* The log for this platform. */
    protected Log log = LogFactory.getLog(getClass());

    /* The platform info. */
    protected DatabasePlatformInfo info = new DatabasePlatformInfo();

    /* The model reader for this platform. */
    protected JdbcModelReader modelReader;

    /* Whether script mode is on. */
    protected boolean scriptModeOn;

    /* Whether SQL comments are generated or not. */
    protected boolean sqlCommentsOn = false;

    /* Whether delimited identifiers are used or not. */
    protected boolean delimitedIdentifierModeOn;

    /* Whether identity override is enabled. */
    protected boolean identityOverrideOn;

    /* Whether read foreign keys shall be sorted alphabetically. */
    protected boolean foreignKeysSorted;

    public AbstractDatabasePlatform() {
    }

    public void setLog(Log log) {
        this.log = log;
    }

    public JdbcModelReader getModelReader() {
        return modelReader;
    }

    public DatabasePlatformInfo getPlatformInfo() {
        return info;
    }

    public boolean isScriptModeOn() {
        return scriptModeOn;
    }

    public void setScriptModeOn(boolean scriptModeOn) {
        this.scriptModeOn = scriptModeOn;
    }

    public boolean isSqlCommentsOn() {
        return sqlCommentsOn;
    }

    public void setSqlCommentsOn(boolean sqlCommentsOn) {
        if (!getPlatformInfo().isSqlCommentsSupported() && sqlCommentsOn) {
            throw new DdlUtilsException("Platform does not support SQL comments");
        }
        this.sqlCommentsOn = sqlCommentsOn;
    }

    public boolean isDelimitedIdentifierModeOn() {
        return delimitedIdentifierModeOn;
    }

    public void setDelimitedIdentifierModeOn(boolean delimitedIdentifierModeOn) {
        if (!getPlatformInfo().isDelimitedIdentifiersSupported() && delimitedIdentifierModeOn) {
            throw new DdlUtilsException("Platform does not support delimited identifier");
        }
        this.delimitedIdentifierModeOn = delimitedIdentifierModeOn;
    }

    public boolean isIdentityOverrideOn() {
        return identityOverrideOn;
    }

    public void setIdentityOverrideOn(boolean identityOverrideOn) {
        this.identityOverrideOn = identityOverrideOn;
    }

    public boolean isForeignKeysSorted() {
        return foreignKeysSorted;
    }

    public void setForeignKeysSorted(boolean foreignKeysSorted) {
        this.foreignKeysSorted = foreignKeysSorted;
    }

    public void createDatabase(DataSource dataSource, Database targetDatabase,
            boolean dropTablesFirst, boolean continueOnError) {
        Database sourceDatabase = new Database();
        Table[] targetTables = targetDatabase.getTables();
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            for (Table targetTable : targetTables) {
                Table sourceTable = readTableFromDatabase(connection, targetTable.getCatalog(),
                        targetTable.getSchema(), targetTable.getName());
                if (sourceTable != null) {
                    sourceDatabase.addTable(sourceTable);
                }
            }
        } catch (SQLException ex) {
            throw new SqlException(ex);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                }
            }
        }

        StringWriter writer = new StringWriter();
        SqlBuilder builder = createSqlBuilder(writer);

        if (builder.isAlterDatabase(sourceDatabase, targetDatabase)) {
            String delimiter = info.getSqlCommandDelimiter();
            builder.alterDatabase(sourceDatabase, targetDatabase);
            String alterSql = writer.toString();
            if (log.isDebugEnabled()) {
                log.debug("TablesAutoUpdatingAlterSql", alterSql);
            }
            new SqlScript(alterSql, dataSource, !continueOnError, delimiter, null).execute();
        }
    }

    public Database readDatabase(Connection connection, String name, String catalog, String schema,
            String[] tableTypes) throws DatabaseOperationException {
        try {
            JdbcModelReader reader = getModelReader();
            Database model = reader.getDatabase(connection, name, catalog, schema, tableTypes);

            postprocessModelFromDatabase(model);
            if ((model.getName() == null) || (model.getName().length() == 0)) {
                model.setName(MODEL_DEFAULT_NAME);
            }
            return model;
        } catch (SQLException ex) {
            throw new DatabaseOperationException(ex);
        }
    }

    public Table readTableFromDatabase(Connection connection, String catalogName,
            String schemaName, String tablename) throws SQLException {
        return postprocessTableFromDatabase(modelReader.readTable(connection, catalogName,
                schemaName, tablename));
    }

    /*
     * Allows the platform to postprocess the model just read from the database.
     * 
     * @param model The model
     */
    protected void postprocessModelFromDatabase(Database model) {
        // Default values for CHAR/VARCHAR/LONGVARCHAR columns have quotation
        // marks
        // around them which we'll remove now
        for (int tableIdx = 0; tableIdx < model.getTableCount(); tableIdx++) {
            postprocessTableFromDatabase(model.getTable(tableIdx));
        }
    }

    protected Table postprocessTableFromDatabase(Table table) {
        if (table != null) {
            for (int columnIdx = 0; columnIdx < table.getColumnCount(); columnIdx++) {
                Column column = table.getColumn(columnIdx);

                if (TypeMap.isTextType(column.getTypeCode())
                        || TypeMap.isDateTimeType(column.getTypeCode())) {
                    String defaultValue = column.getDefaultValue();

                    if ((defaultValue != null) && (defaultValue.length() >= 2)
                            && defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
                        defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
                        column.setDefaultValue(defaultValue);
                    }
                }
            }
        }
        return table;
    }

}
