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


package org.jumpmind.symmetric.job;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.IOfflineServerListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * The default implementation of the {@link IOfflineServerListener}.  
 */
public class DefaultOfflineServerListener implements IOfflineServerListener,
 IBuiltInExtensionPoint {

    protected final static Logger log = LoggerFactory.getLogger(DefaultOfflineServerListener.class);

    protected IStatisticManager statisticManager;
    protected INodeService nodeService;
    protected IOutgoingBatchService outgoingBatchService;
    
    public DefaultOfflineServerListener(IStatisticManager statisticManager,
            INodeService nodeService, IOutgoingBatchService outgoingBatchService) {        
        this.statisticManager = statisticManager;
        this.nodeService = nodeService;
        this.outgoingBatchService = outgoingBatchService;
    }

    /*
     * Handle a client node that was determined to be offline.
     * Syncing is disabled for the node, node security is deleted, and cleanup processing is done for
     * outgoing batches.
     */
    public void clientNodeOffline(Node node) {
        log.warn("Node %s is offline.  Last heartbeat was %s, timezone %s.  Syncing will be disabled and node security deleted.", new Object[] {node.getNodeId(), node.getHeartbeatTime(), node.getTimezoneOffset()});
        statisticManager.incrementNodesDisabled(1);
        node.setSyncEnabled(false);
        nodeService.updateNode(node);
        outgoingBatchService.markAllAsSentForNode(node);
        nodeService.deleteNodeSecurity(node.getNodeId());
    }

    public boolean isAutoRegister() {
        return true;
    }
    
}