/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.db.mssql;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.util.FormatUtils;

public class MsSql2008TriggerTemplate extends MsSql2005TriggerTemplate {
    public MsSql2008TriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
    }

    @Override
    protected String replaceTemplateVariables(DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table originalTable, Table table,
            String defaultCatalog, String defaultSchema, String ddl) {
        ddl = super.replaceTemplateVariables(dml, trigger, history, channel, tablePrefix, originalTable, table, defaultCatalog, defaultSchema, ddl);
        ddl = FormatUtils.replace("anyColumnChanged",
                buildColumnsAreNotEqualString(table, newTriggerValue, oldTriggerValue), ddl);
        return ddl;
    }

    private String buildColumnsAreNotEqualString(Table table, String table1Name, String table2Name) {
        StringBuilder builder = new StringBuilder();
        for (Column column : table.getColumns()) {
            if (builder.length() > 0) {
                builder.append(" or ");
            }
            if (isNotComparable(column)) {
                // Can't compare the value.
                // Let's use the UPDATE() function to see if it showed up in the SET list of the update statement
                builder.append(String.format("UPDATE(\"%1$s\")", column.getName()));
            } else {
                builder.append(String.format("((%1$s.\"%2$s\" IS NOT NULL AND %3$s.\"%2$s\" IS NOT NULL AND %1$s.\"%2$s\"<>%3$s.\"%2$s\") or "
                        + "(%1$s.\"%2$s\" IS NULL AND %3$s.\"%2$s\" IS NOT NULL) or "
                        + "(%1$s.\"%2$s\" IS NOT NULL AND %3$s.\"%2$s\" IS NULL))", table1Name, column.getName(), table2Name));
            }
        }
        if (builder.length() == 0) {
            builder.append("1=1");
        }
        return builder.toString();
    }
}
