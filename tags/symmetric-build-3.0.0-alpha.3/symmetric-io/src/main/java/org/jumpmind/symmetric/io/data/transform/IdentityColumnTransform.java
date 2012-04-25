package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    public static final String NAME = "identity";

    public String getName() {
        return NAME;
    }
    
    
    public boolean isExtractColumnTransform() {
        return false;
    }
    
    public boolean isLoadColumnTransform() {
        return true;
    }

    public String transform(IDatabasePlatform platform, DataContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String newValue, String oldValue)
            throws IgnoreColumnException, IgnoreRowException {  
        if (log.isDebugEnabled()) {
            log.debug("The {} transform requires a generated identity column.  This was configured using the {} target column.", data.getTransformation().getTransformId(), column.getTargetColumnName());
        }
        data.setGeneratedIdentityNeeded(true);
        throw new IgnoreColumnException();
    }

}
