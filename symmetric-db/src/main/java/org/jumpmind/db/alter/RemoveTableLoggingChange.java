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
package org.jumpmind.db.alter;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;

/**
 * Represents the removal of logging mode (turning off) to a table. Subsequent operations will not be logged, if supported by target database.
 */
public class RemoveTableLoggingChange extends TableChangeImplBase {
    public RemoveTableLoggingChange(Table table) {
        super(table);
    }

    @Override
    public void apply(Database database, boolean caseSensitive) {
        Table table = database.findTable(getChangedTable().getName(), caseSensitive);
        table.setLogging(false);
    }
}
