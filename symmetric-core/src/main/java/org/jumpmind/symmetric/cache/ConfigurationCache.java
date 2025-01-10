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
package org.jumpmind.symmetric.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupChannelWindow;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IParameterService;

public class ConfigurationCache {
    private IParameterService parameterService;
    private IConfigurationService configurationService;
    private Object configurationCacheLock = new Object();
    volatile private Map<String, List<NodeChannel>> nodeChannelCache;
    volatile private Map<String, Channel> channelsCache;
    volatile private Collection<String> queuesCache;
    volatile private List<NodeGroupLink> nodeGroupLinksCache;
    volatile private Map<String, List<NodeGroupChannelWindow>> channelWindowsByChannelCache;
    volatile private long channelCacheTime;
    volatile private long nodeChannelCacheTime;
    volatile private long nodeGroupLinkCacheTime;
    volatile private long channelWindowsByChannelCacheTime;

    public ConfigurationCache(ISymmetricEngine engine) {
        this.parameterService = engine.getParameterService();
        this.configurationService = engine.getConfigurationService();
    }

    public List<NodeChannel> getNodeChannels(String nodeId) {
        long channelCacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_CHANNEL_IN_MS);
        List<NodeChannel> nodeChannels;
        synchronized (configurationCacheLock) {
            nodeChannels = nodeChannelCache != null ? nodeChannelCache.get(nodeId) : null;
            if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs || nodeChannels == null) {
                if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs || nodeChannelCache == null) {
                    nodeChannelCache = new HashMap<String, List<NodeChannel>>();
                    nodeChannelCacheTime = System.currentTimeMillis();
                }
                if (nodeId != null) {
                    nodeChannels = configurationService.getNodeChannelsFromDb(nodeId);
                    nodeChannelCache.put(nodeId, nodeChannels);
                } else {
                    nodeChannels = new ArrayList<NodeChannel>(0);
                }
            }
        }
        return nodeChannels;
    }

    public long getNodeChannelCacheTime() {
        return nodeChannelCacheTime;
    }

    public Map<String, Channel> getChannels(boolean refreshCache) {
        checkChannelCache(refreshCache);
        return channelsCache;
    }

    public Collection<String> getQueues(boolean refreshCache) {
        checkChannelCache(refreshCache);
        return queuesCache;
    }

    protected void checkChannelCache(boolean refreshCache) {
        long channelCacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_CHANNEL_IN_MS, 60000);
        if (System.currentTimeMillis() - channelCacheTime >= channelCacheTimeoutInMs || channelsCache == null || refreshCache) {
            synchronized (configurationCacheLock) {
                if (System.currentTimeMillis() - channelCacheTime >= channelCacheTimeoutInMs || channelsCache == null || refreshCache) {
                    channelsCache = configurationService.getChannelsFromDb();
                    Collection<String> queues = new HashSet<String>();
                    for (Channel channel : channelsCache.values()) {
                        queues.add(channel.getQueue());
                    }
                    queuesCache = queues;
                    channelCacheTime = System.currentTimeMillis();
                }
            }
        }
    }

    public List<NodeGroupLink> getNodeGroupLinks(boolean refreshCache) {
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS);
        if (System.currentTimeMillis() - nodeGroupLinkCacheTime >= cacheTimeoutInMs
                || nodeGroupLinksCache == null || refreshCache) {
            synchronized (configurationCacheLock) {
                if (System.currentTimeMillis() - nodeGroupLinkCacheTime >= cacheTimeoutInMs
                        || nodeGroupLinksCache == null || refreshCache) {
                    nodeGroupLinksCache = configurationService.getNodeGroupLinksFromDb();
                    nodeGroupLinkCacheTime = System.currentTimeMillis();
                }
            }
        }
        return nodeGroupLinksCache;
    }

    public Map<String, List<NodeGroupChannelWindow>> getNodeGroupChannelWindows() {
        long channelCacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_CHANNEL_IN_MS, 60000);
        if (System.currentTimeMillis() - channelWindowsByChannelCacheTime >= channelCacheTimeoutInMs || channelWindowsByChannelCache == null) {
            synchronized (configurationCacheLock) {
                if (System.currentTimeMillis() - channelWindowsByChannelCacheTime >= channelCacheTimeoutInMs || channelWindowsByChannelCache == null) {
                    channelWindowsByChannelCache = configurationService.getNodeGroupChannelWindowsFromDb();
                    channelWindowsByChannelCacheTime = System.currentTimeMillis();
                }
            }
        }
        return channelWindowsByChannelCache;
    }

    public void flushNodeChannels() {
        synchronized (configurationCacheLock) {
            nodeChannelCacheTime = 0l;
        }
    }

    public void flushChannels() {
        synchronized (configurationCacheLock) {
            channelCacheTime = 0l;
        }
    }

    public void flushNodeGroupLinks() {
        synchronized (configurationCacheLock) {
            nodeGroupLinkCacheTime = 0l;
        }
    }

    public void flushNodeGroupChannelWindows() {
        synchronized (configurationCacheLock) {
            channelWindowsByChannelCacheTime = 0l;
        }
    }
}
