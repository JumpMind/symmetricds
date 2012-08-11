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
 * under the License. 
 */

package org.jumpmind.symmetric.load;

import org.jumpmind.db.model.Table;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterFilterAdapter;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An out of the box filter that checks to see if the SymmetricDS configuration
 * has changed. If it has, it will take the correct action to apply the
 * configuration change to the current node.
 */
public class ConfigurationChangedFilter extends DatabaseWriterFilterAdapter implements
        IBuiltInExtensionPoint {

    static final Logger log = LoggerFactory.getLogger(ConfigurationChangedFilter.class);

    final String CTX_KEY_RESYNC_NEEDED = "Resync."
            + ConfigurationChangedFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_CHANNELS_NEEDED = "FlushChannels."
            + ConfigurationChangedFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_TRANSFORMS_NEEDED = "FlushTransforms."
            + ConfigurationChangedFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_PARAMETERS_NEEDED = "FlushParameters."
            + ConfigurationChangedFilter.class.getSimpleName() + hashCode();
    
    final String CTX_KEY_FLUSH_CONFLICTS_NEEDED = "FlushConflicts."
            + ConfigurationChangedFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_RESTART_JOBMANAGER_NEEDED = "RestartJobManager."
            + ConfigurationChangedFilter.class.getSimpleName() + hashCode();

    private ISymmetricEngine engine;

    public ConfigurationChangedFilter(ISymmetricEngine engine) {
        this.engine = engine;
    }

    @Override
    public void afterWrite(DataContext context, Table table, CsvData data) {
        recordSyncNeeded(context, table, data);
        recordChannelFlushNeeded(context, table);
        recordTransformFlushNeeded(context, table);
        recordParametersFlushNeeded(context, table);
        recordJobManagerRestartNeeded(context, table, data);
        recordConflictFlushNeeded(context, table);
    }

    private void recordSyncNeeded(DataContext context, Table table, CsvData data) {
        if (isSyncTriggersNeeded(table)) {
            context.put(CTX_KEY_RESYNC_NEEDED, true);
        }
        
        if (data.getDataEventType() == DataEventType.CREATE) {
            engine.getTriggerRouterService().syncTriggers(table, false);
        }
    }

    private void recordJobManagerRestartNeeded(DataContext context, Table table, CsvData data) {
        if (isJobManagerRestartNeeded(table, data)) {
            context.put(CTX_KEY_RESTART_JOBMANAGER_NEEDED, true);
        }
    }
    
    private void recordConflictFlushNeeded(DataContext context, Table table) {
        if (isConflictFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_CONFLICTS_NEEDED, true);
        }
    }

    private void recordParametersFlushNeeded(DataContext context, Table table) {
        if (isParameterFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_PARAMETERS_NEEDED, true);
        }
    }

    private void recordChannelFlushNeeded(DataContext context, Table table) {
        if (isChannelFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_CHANNELS_NEEDED, true);
        }
    }

    private void recordTransformFlushNeeded(DataContext context, Table table) {
        if (isTransformFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_TRANSFORMS_NEEDED, true);
        }
    }

    private boolean isSyncTriggersNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_TRIGGER)
                || matchesTable(table, TableConstants.SYM_ROUTER)
                || matchesTable(table, TableConstants.SYM_TRIGGER_ROUTER)
                || matchesTable(table, TableConstants.SYM_NODE_GROUP_LINK);
    }

    private boolean isChannelFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_CHANNEL);
    }
    
    private boolean isConflictFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_CONFLICT);
    }

    private boolean isParameterFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_PARAMETER);
    }

    private boolean isJobManagerRestartNeeded(Table table, CsvData data) {
        return matchesTable(table, TableConstants.SYM_PARAMETER)
                && data.getCsvData(CsvData.ROW_DATA) != null
                && data.getCsvData(CsvData.ROW_DATA).contains("job.");
    }

    private boolean isTransformFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_TRANSFORM_COLUMN)
                || matchesTable(table, TableConstants.SYM_TRANSFORM_TABLE);
    }

    private boolean matchesTable(Table table, String tableSuffix) {
        if (table != null && table.getName() != null) {
            return table.getName().equalsIgnoreCase(
                    TableConstants.getTableName(engine.getParameterService().getTablePrefix(),
                            tableSuffix));
        } else {
            return false;
        }
    }

    @Override
    public void batchCommitted(DataContext context) {
        IParameterService parameterService = engine.getParameterService();
        if (context.get(CTX_KEY_FLUSH_CHANNELS_NEEDED) != null) {
            log.info("Channels flushed because new channels came through the data loader");
            engine.getConfigurationService().reloadChannels();
        }
        
        if (context.get(CTX_KEY_RESYNC_NEEDED) != null
                && parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            log.info("About to syncTriggers because new configuration came through the data loader");
            engine.getTriggerRouterService().syncTriggers();
        }
        
        if (context.get(CTX_KEY_FLUSH_TRANSFORMS_NEEDED) != null) {
            log.info("About to refresh the cache of transformation because new configuration came through the data loader");
            engine.getTransformService().resetCache();
        }

        if (context.get(CTX_KEY_FLUSH_CONFLICTS_NEEDED) != null) {
            log.info("About to refresh the cache of conflict settings because new configuration came through the data loader");
            engine.getDataLoaderService().reloadConflictNodeGroupLinks();
        }

        if (context.get(CTX_KEY_FLUSH_PARAMETERS_NEEDED) != null) {
            log.info("About to refresh the cache of parameters because new configuration came through the data loader");
            parameterService.rereadParameters();
        }

        if (context.get(CTX_KEY_RESTART_JOBMANAGER_NEEDED) != null) {
            IJobManager jobManager = engine.getJobManager();
            if (jobManager != null) {
                log.info("About to restart jobs because a new schedule came through the data loader");
                jobManager.stopJobs();
                jobManager.startJobs();
            }
        }
    }
}
