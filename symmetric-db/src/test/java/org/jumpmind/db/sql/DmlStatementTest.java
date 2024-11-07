package org.jumpmind.db.sql;

import org.jumpmind.db.util.BinaryEncoding;
import org.junit.jupiter.api.Test;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.*;
import org.jumpmind.db.sql.DmlStatement.DmlType;

import static org.junit.Assert.assertEquals;

public class DmlStatementTest {
    @Test
    public void testBuildDynamicSqlUpdateNulls() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabase.xml"));
        Table table = database.getTable(0);
        DmlStatementOptions options = new DmlStatementOptions(DmlType.UPDATE, table);
        DmlStatement dml = new DmlStatement(options);
        Row row = new Row(2);
        String sql = dml.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
        String expectedSql = "update test_simple_table set id = null, string_one_value = null where id is null;";
        assertEquals(expectedSql, sql);
    }

    @Test
    public void testBuildDynamicSqlSelectNulls() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabase.xml"));
        Table table = database.getTable(0);
        DmlStatementOptions options = new DmlStatementOptions(DmlType.SELECT, table);
        DmlStatement dml = new DmlStatement(options);
        Row row = new Row(2);
        String sql = dml.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
        String expectedSql = "select id, string_one_value from test_simple_table where id is null;";
        assertEquals(expectedSql, sql);
    }

    @Test
    public void testBuildDynamicSqlDeleteNulls() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabase.xml"));
        Table table = database.getTable(0);
        DmlStatementOptions options = new DmlStatementOptions(DmlType.DELETE, table);
        DmlStatement dml = new DmlStatement(options);
        Row row = new Row(2);
        String sql = dml.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
        String expectedSql = "delete from test_simple_table where id is null;";
        assertEquals(expectedSql, sql);
    }

    @Test
    public void testBuildDynamicSqlInsertNulls() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabase.xml"));
        Table table = database.getTable(0);
        DmlStatementOptions options = new DmlStatementOptions(DmlType.INSERT, table);
        DmlStatement dml = new DmlStatement(options);
        Row row = new Row(2);
        String sql = dml.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
        String expectedSql = "insert into test_simple_table (id, string_one_value) values (null,null);";
        assertEquals(expectedSql, sql);
    }

    @Test
    public void testBuildDynamicSqlWhereNulls() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabase.xml"));
        Table table = database.getTable(0);
        DmlStatementOptions options = new DmlStatementOptions(DmlType.WHERE, table);
        DmlStatement dml = new DmlStatement(options);
        Row row = new Row(2);
        String sql = dml.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
        String expectedSql = "where id is null;";
        assertEquals(expectedSql, sql);
    }

    @Test
    public void testBuildDynamicSqlUpdateNonNulls() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabase.xml"));
        Table table = database.getTable(0);
        DmlStatementOptions options = new DmlStatementOptions(DmlType.UPDATE, table);
        DmlStatement dml = new DmlStatement(options);
        Row row = new Row(2);
        row.put("id", 1);
        row.put("string_one_value", "test");
        String sql = dml.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
        String expectedSql = "update test_simple_table set id = 1, string_one_value = 'test' where id = 1;";
        assertEquals(expectedSql, sql);
    }

    @Test
    public void testBuildDynamicSqlSelectNonNulls() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabase.xml"));
        Table table = database.getTable(0);
        DmlStatementOptions options = new DmlStatementOptions(DmlType.SELECT, table);
        DmlStatement dml = new DmlStatement(options);
        Row row = new Row(2);
        row.put("id", 1);
        row.put("string_one_value", "test");
        String sql = dml.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
        String expectedSql = "select id, string_one_value from test_simple_table where id = 1;";
        assertEquals(expectedSql, sql);
    }

    @Test
    public void testBuildDynamicSqlWithTimestamp() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabase.xml"));
        Table table = database.getTable(1);
        DmlStatementOptions options = new DmlStatementOptions(DmlType.UPDATE, table);
        DmlStatement dml = new DmlStatement(options);
        Row row = new Row(1);
        row.put("ts", "2020-01-01 15:10:10");
        String sql = dml.buildDynamicSql(BinaryEncoding.HEX, row, false, true);
        String expectedSql = "update testColumnWithTimestamp set ts = {ts '2020-01-01 15:10:10.000'};";
        assertEquals(expectedSql, sql);
    }
}
