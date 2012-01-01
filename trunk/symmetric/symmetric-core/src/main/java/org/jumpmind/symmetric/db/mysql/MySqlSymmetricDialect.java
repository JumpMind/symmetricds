/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */


package org.jumpmind.symmetric.db.mysql;

import java.sql.Connection;
import java.sql.SQLException;

import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

public class MySqlSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    private static final String TRANSACTION_ID = "transaction_id";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "@sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "@sync_node_disabled";
    
    private String functionTemplateKeySuffix = null;

    public MySqlSymmetricDialect() {
        this.triggerText = new MySqlTriggerText();
    }
    
    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
        int[] versions = Version.parseVersion(getProductVersion());
        if (getMajorVersion() == 5 && (getMinorVersion() == 0 || (getMinorVersion() == 1 && versions[2] < 23))) {
            this.functionTemplateKeySuffix = "_pre_5_1_23";
        } else {
            this.functionTemplateKeySuffix = "_post_5_1_23";
        }
    }
    
    @Override
    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    protected void createRequiredFunctions() {
        String[] functions = triggerText.getFunctionsToInstall();
        for (int i = 0; i < functions.length; i++) {
            if (functions[i].endsWith(this.functionTemplateKeySuffix)) {
                String funcName = tablePrefix + "_"
                        + functions[i].substring(0, functions[i].length() - this.functionTemplateKeySuffix.length());
                if (jdbcTemplate.queryForInt(triggerText.getFunctionInstalledSql(funcName, platform.getDefaultSchema())) == 0) {
                    jdbcTemplate.update(triggerText.getFunctionSql(functions[i], funcName, platform.getDefaultSchema()));
                    log.info("FunctionInstalled", funcName);
                }
            }
        }
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName, String triggerName) {
        catalog = catalog == null ? (platform.getDefaultCatalog() == null ? null : platform.getDefaultCatalog()) : catalog;
        String checkCatalogSql = (catalog != null && catalog.length() > 0) ? " and trigger_schema='" + catalog + "'"
                : "";
        return jdbcTemplate.queryForInt(
                "select count(*) from information_schema.triggers where trigger_name like ? and event_object_table like ?"
                        + checkCatalogSql, new Object[] { triggerName, tableName }) > 0;
    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName, String triggerName,
            String tableName, TriggerHistory oldHistory) {
        catalogName = catalogName == null ? "" : (catalogName + ".");
        final String sql = "drop trigger " + catalogName + triggerName;
        logSql(sql, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
                jdbcTemplate.update(sql);
            } catch (Exception e) {
                log.warn("TriggerDoesNotExist");
            }
        }
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        transaction.execute("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=1");
        if (nodeId != null) {
            transaction.execute("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "='" + nodeId + "'");
        }
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.execute("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=null");
        transaction.execute("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "=null");
    }

    public String getSyncTriggersExpression() {
        return SYNC_TRIGGERS_DISABLED_USER_VARIABLE + " is null";
    }

    private final String getTransactionFunctionName() {
        return platform.getDefaultCatalog() + "." + tablePrefix + "_" + TRANSACTION_ID;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return getTransactionFunctionName() + "()";
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select last_insert_id()";
    }

    public void purge() {
    }

    @Override
    protected String switchCatalogForTriggerInstall(String catalog, Connection c) throws SQLException {
        if (catalog != null) {
            String previousCatalog = c.getCatalog();
            c.setCatalog(catalog);
            return previousCatalog;
        } else {
            return null;
        }
    }

    /*
     * According to the documentation (and experience) the jdbc driver for mysql
     * requires the fetch size to be as follows.
     */
    @Override
    public int getStreamingResultsFetchSize() {
        return Integer.MIN_VALUE;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    @Override
    protected String getDbSpecificDataHasChangedCondition(Trigger trigger) {
        return "var_row_data != var_old_data";
    }
}