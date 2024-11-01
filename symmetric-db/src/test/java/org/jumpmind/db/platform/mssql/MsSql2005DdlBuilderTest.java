package org.jumpmind.db.platform.mssql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Types;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class MsSql2005DdlBuilderTest {
    @BeforeAll
    static void setUpBeforeClass() throws Exception {
    }

    // @ParameterizedTest
    // @CsvSource({
    // "LONGNVARCHAR,NVARCHAR(MAX)",
    // "VARCHAR,NVARCHAR"
    // })

    record NativeTypesInputTuple(int jdbcTypeCode, String nativeTypeName) {
    }

    public static NativeTypesInputTuple[] paramNativeTypes() {
        ArrayList<NativeTypesInputTuple> inputs = new ArrayList<NativeTypesInputTuple>();
        inputs.add(new NativeTypesInputTuple(Types.LONGNVARCHAR, "NVARCHAR(MAX)"));
        // inputs.add(new NativeTypesInputTuple(Types.VARCHAR, "NVARCHAR"));
        return (inputs.toArray(new NativeTypesInputTuple[0]));
    }

    @ParameterizedTest
    @MethodSource(value = "paramNativeTypes")
    void testReadXml_ConvertedToNVarchar(NativeTypesInputTuple inputTuple) {
        // String xml = "<database name=\"test\">"
        // + "<table name=\"testWithNVarchar\" logging=\">\n"
        // + " <column name=\"col1\" type=\"VARCHAR\" size=\"4000\"/>\n"
        // + " <column name=\"col2\" type=\"VARCHAR\" size=\"8000\"/>\n"
        // + " <column name=\"col3\" type=\"VARCHAR\" size=\"2147483647\"/>\n"
        // + " <column name=\"col4\" type=\"LONGVARCHAR\" size=\"2147483647\"/>\n"
        // + " <column name=\"col5\" type=\"LONGNVARCHAR\" size=\"2147483647\"/>\n"
        // + " </table></database>\n";
        // StringReader reader = new StringReader(xml);
        // Database database = DatabaseXmlUtil.read(reader, false);
        // Table table = database.getTable(0);
        // assertTrue(table != null);
        WrapperMsSql2005DdlBuilder ddlBuilder = new WrapperMsSql2005DdlBuilder();
        // assertEquals(Types.LONGNVARCHAR, inputTuple.jdbcTypeCode());
//        assertEquals(Types.VARCHAR, inputTuple.jdbcTypeCode());
        String nativeType = ddlBuilder.getNativeType(inputTuple.jdbcTypeCode());
        assertEquals(inputTuple.nativeTypeName(), nativeType);
    }

    protected class WrapperMsSql2005DdlBuilder extends MsSql2005DdlBuilder {
        public WrapperMsSql2005DdlBuilder() {
        }

        // Helps with access to private member
        public String getNativeType(int typeCode) {
            return this.databaseInfo.getNativeType(typeCode);
        }

        // Helps with access to private member
        public int getTargetJdbcType(int typeCode) {
            return this.databaseInfo.getTargetJdbcType(typeCode);
        }
    }
}
