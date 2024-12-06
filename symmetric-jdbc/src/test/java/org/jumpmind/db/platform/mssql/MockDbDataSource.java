package org.jumpmind.db.platform.mssql;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.jumpmind.db.DdlReaderTestConstants;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.mockito.ArgumentMatchers;

import com.nuodb.jdbc.EmptyResultSet;

/**
 * Main class in the hierarchy of Mock classes allowing to inject any results into test subject (DatabasePlatform, DdlReader, DdlBuilder, etc.).
 */
public class MockDbDataSource implements DataSource {
    protected MockDbConnection mockConnection;
    protected MockDbMetaData mockDbMetaData;
    protected ArrayList<ResultSet> metaDataSets;
    protected ArrayList<MockDbStatement> statements;
    protected ArrayList<MockDbPreparedStatement> preparedStatements;
    protected SqlTemplateSettings sqlTemplateSettings;

    public MockDbDataSource(int databaseMajorVersion) {
        this.mockConnection = new MockDbConnection(this);
        this.mockDbMetaData = new MockDbMetaData(this, databaseMajorVersion);
        this.metaDataSets = new ArrayList<ResultSet>();
        this.statements = new ArrayList<MockDbStatement>();
        this.preparedStatements = new ArrayList<MockDbPreparedStatement>();
        this.sqlTemplateSettings = new SqlTemplateSettings();
        this.sqlTemplateSettings.setQueryTimeout(0);
    }

    public MockDbDataSource(int databaseMajorVersion, SqlTemplateSettings sqlTemplateSettings) {
        this.mockConnection = new MockDbConnection(this);
        this.mockDbMetaData = new MockDbMetaData(this, databaseMajorVersion);
        this.statements = new ArrayList<MockDbStatement>();
        this.preparedStatements = new ArrayList<MockDbPreparedStatement>();
        this.sqlTemplateSettings = sqlTemplateSettings;
    }

    public int getDatabaseMajorVersion() throws SQLException {
        return this.mockDbMetaData.getDatabaseMajorVersion();
    }

    public SqlTemplateSettings getSqlTemplateSettings() {
        return this.sqlTemplateSettings;
    }

    public DatabaseMetaData getMetaData() {
        return this.mockDbMetaData;
    }

    public void enqueue(MockDbPreparedStatement preparedStatement) {
        this.preparedStatements.add(preparedStatement);
    }

    public void enqueue(MockDbStatement statement) {
        this.statements.add(statement);
    }

    public void enqueueMetaData(ResultSet resultSet) {
        this.metaDataSets.add(resultSet);
    }

    public ResultSet dequeueMetaDataResultSet() {
        if (this.metaDataSets.size() < 1) {
            System.out.println("dequeuePreparedStatement - No more pre-defined results in the metaData queue!");
            return null;
        }
        ResultSet resultSet = this.metaDataSets.remove(0);
        System.out.println("dequeueMetaDataResultSet - Dispensed one resultSet entry from the metaData queue;");
        return resultSet;
    }

    public MockDbPreparedStatement dequeuePreparedStatement() {
        if (this.preparedStatements.size() < 1) {
            System.out.println("dequeuePreparedStatement - No more pre-defined results in the preparedStatements queue!");
            return null;
        }
        MockDbPreparedStatement preparedStatement = this.preparedStatements.remove(0);
        // System.out.println("dequeuePreparedStatement - Dispensed one item from the preparedStatements queue;");
        return preparedStatement;
    }

    public MockDbStatement dequeueStatement() {
        if (this.statements.size() < 1) {
            System.out.println("dequeueStatement - No more pre-defined results in the statements queue!");
            return null;
        }
        MockDbStatement statement = this.statements.remove(0);
        // System.out.println("dequeueStatement - Dispensed one item from the statements queue;");
        return statement;
    }
    // public ISqlTemplate createMockSqlTemplate(DatabaseInfo databaseInfo) {
    // MsSqlJdbcSqlTemplate testTemplate = new MsSqlJdbcSqlTemplate(this, this.sqlTemplateSettings, databaseInfo);
    // MsSqlJdbcSqlTemplate sqlTemplate = Mockito.spy(testTemplate);
    // doReturn(this.databaseMajorVersion).when(sqlTemplate).getDatabaseMajorVersion();
    // doReturn(this).when(sqlTemplate).getDataSource();
    // return sqlTemplate;
    // }
    //
    // public ISqlTemplate createMockSqlTemplateDirty(DatabaseInfo databaseInfo) {
    // MsSqlJdbcSqlTemplate testTemplate = new MsSqlJdbcSqlTemplate(this, this.sqlTemplateSettings, databaseInfo);
    // testTemplate.setIsolationLevel(1);
    // MsSqlJdbcSqlTemplate sqlTemplate = Mockito.spy(testTemplate);
    // doReturn(this.databaseMajorVersion).when(sqlTemplate).getDatabaseMajorVersion();
    // doReturn(this).when(sqlTemplate).getDataSource();
    // // sqlTemplate.setIsolationLevel(1);
    // // doReturn(1).when(sqlTemplate).getIsolationLevel();
    // return sqlTemplate;
    // }

    // this.ddlBuilder = createDdlBuilder();
    // this.sqlTemplate = createSqlTemplate();
    // this.sqlTemplateDirty = createSqlTemplateDirty();
    // this.ddlReader = createDdlReader();
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return mockConnection;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return mockConnection;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        // TODO Auto-generated method stub
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        // TODO Auto-generated method stub
    }

    /**
     * Helper - mocks up a null result set for a PreparedStatement (no data returned!). Use with MockDbDataSource.enqueue()
     */
    public static MockDbPreparedStatement buildPreparedStatementNoResults(String sql, int repeatOutput) {
        return buildPreparedStatement(sql, new EmptyResultSet(), repeatOutput);
    }

    /**
     * Helper - mocks up a PreparedStatement with specified results. Use with MockDbDataSource.enqueue()
     */
    public static MockDbPreparedStatement buildPreparedStatement(String sql, ResultSet mockResultSet, int repeatOutput) {
        MockDbPreparedStatement statement = new MockDbPreparedStatement(sql, mockResultSet, repeatOutput);
        return statement;
    }

    /**
     * Helper - mocks up a null result set for a Statement (no data returned!). Use with MockDbDataSource.enqueue()
     */
    public static MockDbStatement buildStatementNoResults(String sql, int repeatOutput) {
        return buildStatement(sql, new EmptyResultSet(), repeatOutput);
    }

    /**
     * Helper - mocks up a Statement with specified results. Use with MockDbDataSource.enqueue()
     */
    public static MockDbStatement buildStatement(String sql, ResultSet mockResultSet, int repeatOutput) {
        MockDbStatement statement = new MockDbStatement(sql, mockResultSet, repeatOutput);
        return statement;
    }

    /**
     * Helper - mocks up meta data ResultSet for a table (targeting getTables call). Use with MockDbDataSource.enqueueMetaData()
     */
    public static ResultSet buildTableHeaderMetaData(
            String catalog,
            String schema,
            String tableName,
            String tableType,
            String description) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(false);
        ResultSetMetaData rsMetaData = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(rsMetaData);
        when(rsMetaData.getColumnCount()).thenReturn(5);
        // Mock column 1==TABLE_NAME
        when(rsMetaData.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData.getColumnName(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs.getString(1)).thenReturn(tableName);
        // Mock column 2==TABLE_TYPE
        when(rsMetaData.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rsMetaData.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rs.getString(2)).thenReturn(tableType);
        // Mock column 3==TABLE_CAT
        when(rsMetaData.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rsMetaData.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rs.getString(3)).thenReturn(catalog);
        // Mock column 4==TABLE_SCHEM
        when(rsMetaData.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rsMetaData.getColumnName(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rs.getString(4)).thenReturn(schema);
        // Mock column 5==REMARKS
        when(rsMetaData.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rsMetaData.getColumnName(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rs.getString(5)).thenReturn(description);
        return rs;

    }

    /**
     * Helper - mocks up meta data ResultSet describing a 1-column table (targeting getColumns call). Use with MockDbDataSource.enqueueMetaData()
     */
    public static ResultSet buildTable1ColumnMetaData(
            String columnName,
            String defaultValue, String jdbcTypeName, int jdbcTypeCode, String testColumnSize,
            boolean isNullable) throws SQLException {
        ResultSet rs2 = mock(ResultSet.class);
        ResultSetMetaData rsMetaData2 = mock(ResultSetMetaData.class);
        when(rs2.next()).thenReturn(true).thenReturn(false);
        when(rs2.getMetaData()).thenReturn(rsMetaData2);
        when(rsMetaData2.getColumnCount()).thenReturn(7);
        // Mock column 1==COLUMN_DEF
        when(rsMetaData2.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rsMetaData2.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rs2.getString(1)).thenReturn(defaultValue);
        // Mock column 2==COLUMN_DEFAULT
        when(rsMetaData2.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rsMetaData2.getColumnName(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rs2.getString(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT_TEST_VALUE);
        // Mock column 3==TABLE_NAME
        when(rsMetaData2.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData2.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs2.getString(3)).thenReturn(DdlReaderTestConstants.TESTNAME);
        // Mock column 4==COLUMN_NAME
        when(rsMetaData2.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData2.getColumnName(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs2.getString(4)).thenReturn(columnName);
        // Mock column 5==TYPE_NAME
        when(rsMetaData2.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rsMetaData2.getColumnName(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rs2.getString(5)).thenReturn(jdbcTypeName);
        // Mock column 6==DATA_TYPE
        when(rsMetaData2.getColumnLabel(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rsMetaData2.getColumnName(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rs2.getInt(6)).thenReturn(jdbcTypeCode);
        // Mock column 7==IS_NULLABLE
        when(rsMetaData2.getColumnLabel(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rsMetaData2.getColumnName(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rs2.getString(7)).thenReturn(isNullable ? "TRUE" : "FALSE");
        return rs2;
    }

    /**
     * Helper - mocks up meta data ResultSet describing a 1-primary key table with one column (targeting getPrimaryKeys call). Use with
     * MockDbDataSource.enqueueMetaData()
     */
    public static ResultSet buildTable1PrimaryKey1ColumnMetaData(
            String tableName, String primaryKeyName, String columnName) throws SQLException {
        ResultSet rs4 = mock(ResultSet.class);
        ResultSetMetaData rsMetaData4 = mock(ResultSetMetaData.class);
        when(rs4.getMetaData()).thenReturn(rsMetaData4);
        when(rs4.next()).thenReturn(true).thenReturn(false);
        when(rsMetaData4.getColumnCount()).thenReturn(3);
        // Mock column 1==COLUMN_NAME
        when(rsMetaData4.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData4.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs4.getString(1)).thenReturn(columnName);
        // Mock column 2==TABLE_NAME
        when(rsMetaData4.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData4.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs4.getString(2)).thenReturn(tableName);
        // Mock column 3==PK_NAME
        when(rsMetaData4.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rsMetaData4.getColumnName(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rs4.getString(3)).thenReturn(primaryKeyName);
        return rs4;
    }

    /**
     * Helper - mocks up meta data ResultSet describing a 1-index table (targeting getIndexInfo call). Use with MockDbDataSource.enqueueMetaData()
     */
    public static ResultSet buildTable1IndexInfoMetaData(
            String indexName, String tableName) throws SQLException {
        ResultSet rs5 = mock(ResultSet.class);
        ResultSetMetaData rsMetaData5 = mock(ResultSetMetaData.class);
        when(rs5.next()).thenReturn(true).thenReturn(false);
        when(rs5.getMetaData()).thenReturn(rsMetaData5);
        when(rsMetaData5.getColumnCount()).thenReturn(2);
        // Mock column 1==INDEX_NAME
        when(rsMetaData5.getColumnLabel(1)).thenReturn("INDEX_NAME");
        when(rsMetaData5.getColumnName(1)).thenReturn("INDEX_NAME");
        when(rs5.getString(1)).thenReturn(indexName);
        // Mock column 2==TABLE_NAME
        when(rsMetaData5.getColumnLabel(2)).thenReturn("TABLE_NAME");
        when(rsMetaData5.getColumnName(2)).thenReturn("TABLE_NAME");
        when(rs5.getString(2)).thenReturn(tableName);
        return rs5;
    }

    /**
     * Helper - mocks up meta data ResultSet describing the AutoIncrementColumn column meta data (targeting isAutoIncrement call). Use with
     * MockDbDataSource.enqueueMetaData()
     */
    public static ResultSet buildAutoIncrement1ColumnMetaData(
            boolean isAutoIncrement) throws SQLException {
        ResultSet stmtrs1 = mock(ResultSet.class);
        ResultSetMetaData stmt1RsMetaData = mock(ResultSetMetaData.class);
        when(stmtrs1.next()).thenReturn(true).thenReturn(false);
        when(stmtrs1.getMetaData()).thenReturn(stmt1RsMetaData);
        when(stmt1RsMetaData.isAutoIncrement(ArgumentMatchers.anyInt())).thenReturn(isAutoIncrement);
        // when(stmtrs1.getString(1)).thenReturn(DdlReaderTestConstants.TESTNAMECAPS);
        return stmtrs1;
    }

    /**
     * Helper - mocks up a generic ResultSet with zero columns and zero rows.
     */
    public static ResultSet buildResultSet0Columns0Rows() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(false);
        ResultSetMetaData rsMetaData = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(rsMetaData);
        when(rsMetaData.getColumnCount()).thenReturn(0);
        when(rsMetaData.getColumnLabel(ArgumentMatchers.anyInt())).thenReturn("0Columns0Rows");
        when(rs.getString(ArgumentMatchers.anyInt())).thenReturn("0Columns0Rows");
        return rs;
    }

    /**
     * Helper - mocks up a generic ResultSet with only one column and one row.
     */
    public static ResultSet buildResultSet1Column1RowStringValue(String columnName, String value) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(false);
        ResultSetMetaData rsMetaData = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(rsMetaData);
        when(rsMetaData.getColumnCount()).thenReturn(1);
        when(rsMetaData.getColumnLabel(1)).thenReturn(columnName);
        when(rs.getString(1)).thenReturn(value);
        when(rs.getObject(1)).thenReturn(value);
        return rs;
    }

}
