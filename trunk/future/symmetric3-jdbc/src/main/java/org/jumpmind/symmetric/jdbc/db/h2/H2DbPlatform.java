package org.jumpmind.symmetric.jdbc.db.h2;

import java.sql.Types;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.h2.H2SqlBuilder;
import org.jumpmind.symmetric.core.db.h2.H2TriggerBuilder;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.jdbc.db.AbstractJdbcDbPlatform;

public class H2DbPlatform extends AbstractJdbcDbPlatform {

    protected static final int[] DATA_INTEGRITY_SQL_ERROR_CODES = { 22003, 22012, 22025, 23000,
            23001, 23505, 90005 };

    public H2DbPlatform(DataSource dataSource, Parameters parameters) {
        super(dataSource, parameters);

        platformInfo.setNonPKIdentityColumnsSupported(false);
        platformInfo.setIdentityOverrideAllowed(false);
        platformInfo.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        platformInfo.setNullAsDefaultValueRequired(false);
        platformInfo.addNativeTypeMapping(Types.ARRAY, "BINARY", Types.BINARY);
        platformInfo.addNativeTypeMapping(Types.DISTINCT, "BINARY", Types.BINARY);
        platformInfo.addNativeTypeMapping(Types.NULL, "BINARY", Types.BINARY);
        platformInfo.addNativeTypeMapping(Types.REF, "BINARY", Types.BINARY);
        platformInfo.addNativeTypeMapping(Types.STRUCT, "BINARY", Types.BINARY);
        platformInfo.addNativeTypeMapping(Types.DATALINK, "BINARY", Types.BINARY);

        platformInfo.addNativeTypeMapping(Types.BIT, "BOOLEAN", Types.BIT);
        
        platformInfo.addNativeTypeMapping(Types.NUMERIC, "DECIMAL", Types.DECIMAL);
        platformInfo.addNativeTypeMapping(Types.BINARY, "BINARY", Types.BINARY);
        platformInfo.addNativeTypeMapping(Types.BLOB, "BLOB", Types.BLOB);
        platformInfo.addNativeTypeMapping(Types.CLOB, "CLOB", Types.CLOB);
        platformInfo.addNativeTypeMapping(Types.LONGVARCHAR, "VARCHAR", Types.VARCHAR);
        platformInfo.addNativeTypeMapping(Types.FLOAT, "DOUBLE", Types.DOUBLE);
        platformInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "OTHER");

        platformInfo.setDefaultSize(Types.CHAR, Integer.MAX_VALUE);
        platformInfo.setDefaultSize(Types.VARCHAR, Integer.MAX_VALUE);
        platformInfo.setDefaultSize(Types.BINARY, Integer.MAX_VALUE);
        platformInfo.setDefaultSize(Types.VARBINARY, Integer.MAX_VALUE);

        platformInfo.setDateOverridesToTimestamp(false);
        platformInfo.setEmptyStringNulled(false);
        platformInfo.setBlankCharColumnSpacePadded(true);
        platformInfo.setNonBlankCharColumnSpacePadded(false);
        platformInfo.setRequiresAutoCommitFalseToSetFetchSize(false);
        
        this.jdbcModelReader = new H2JdbcModelReader(this);
        this.sqlBuilder = new H2SqlBuilder(this);
        this.triggerBuilder = new H2TriggerBuilder(this);

    }

    @Override
    protected int[] getDataIntegritySqlErrorCodes() {
        return DATA_INTEGRITY_SQL_ERROR_CODES;
    }

}
