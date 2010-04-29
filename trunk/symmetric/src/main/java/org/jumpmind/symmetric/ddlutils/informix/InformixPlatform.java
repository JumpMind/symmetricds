package org.jumpmind.symmetric.ddlutils.informix;

import java.sql.Types;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.platform.PlatformImplBase;

public class InformixPlatform extends PlatformImplBase implements Platform {

    public static final String DATABASENAME = "Informix Dynamic Server11";
    
    public static final String JDBC_DRIVER = "com.informix.jdbc.IfxDriver";
    
    public static final String JDBC_SUBPROTOCOL = "informix-sqli";

    public InformixPlatform() {
        PlatformInfo info = getPlatformInfo();

        info.addNativeTypeMapping(Types.VARCHAR, "VARCHAR", Types.VARCHAR);
        info.addNativeTypeMapping(Types.LONGVARCHAR, "LVARCHAR", Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.TIMESTAMP, "DATETIME YEAR TO FRACTION", Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.TIME, "DATETIME YEAR TO FRACTION", Types.TIME);
        info.addNativeTypeMapping(Types.BINARY, "BYTE", Types.BINARY);
        info.addNativeTypeMapping(Types.VARBINARY, "BYTE", Types.BINARY);

        info.addNativeTypeMapping(Types.BIT, "BOOLEAN", Types.BIT);
        info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.TINYINT);
        info.addNativeTypeMapping(Types.DOUBLE, "FLOAT", Types.DOUBLE);
        
        setSqlBuilder(new InformixBuilder(this));
        setModelReader(new InformixModelReader(this));
    }

    public String getName() {
        return DATABASENAME;
    }
}
