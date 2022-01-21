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
package org.jumpmind.symmetric.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.model.Node;
import org.junit.jupiter.api.Test;

public class ConfigurationVersionHelperTest {
    protected final String PREFIX = "sym";

    @Test
    public void testFilterNodesOss() {
        testFilterNodes(false);
    }

    @Test
    public void testFilterNodesPro() {
        testFilterNodes(true);
    }

    public void testFilterNodes(boolean isPro) {
        ConfigurationVersionHelper helper = new ConfigurationVersionHelper(PREFIX);
        Map<String, List<String>> versionWithTables = getVersionWithTables();
        List<String> versions = new ArrayList<String>(versionWithTables.keySet());
        versions.sort(new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return s1.equals(s2) ? 0 : Version.isOlderThanVersion(s1, s2) ? -1 : 1;
            }
        });
        Set<Node> nodes = new HashSet<Node>();
        for (String version : versions) {
            Node node = new Node(version, version);
            node.setSymmetricVersion(version);
            node.setDeploymentType(isPro ? Constants.DEPLOYMENT_TYPE_PROFESSIONAL : Constants.DEPLOYMENT_TYPE_SERVER);
            nodes.add(node);
        }
        List<String> shouldSendTables = TableConstants.getConfigTables(PREFIX);
        List<String> shouldNotSendTables = new ArrayList<String>();
        for (String table : TableConstants.getConfigTablesByVersion(PREFIX).keySet()) {
            shouldSendTables.remove(table);
            shouldNotSendTables.add(table);
        }
        Set<String> proTables = TableConstants.getTablesForConsole(PREFIX);
        for (String version : versions) {
            List<String> newTables = versionWithTables.get(version);
            if (newTables != null) {
                for (String table : newTables) {
                    if (isPro || !proTables.contains(table)) {
                        shouldSendTables.add(table);
                        shouldNotSendTables.remove(table);
                    }
                }
            }
            for (String table : shouldSendTables) {
                Set<Node> filteredNodes = helper.filterNodes(nodes, table);
                assertTrue("Should send table " + table + " to node " + version, filteredNodes.contains(new Node(version, version)));
            }
            for (String table : shouldNotSendTables) {
                Set<Node> filteredNodes = helper.filterNodes(nodes, table);
                assertFalse("Should NOT send table " + table + " to node " + version, filteredNodes.contains(new Node(version, version)));
            }
        }
    }

    @Test
    public void testShouldSendTableOss() {
        testShouldSendTable(false);
    }

    @Test
    public void testShouldSendTablePro() {
        testShouldSendTable(true);
    }

    public void testShouldSendTable(boolean isPro) {
        Map<String, List<String>> versionWithTables = getVersionWithTables();
        List<String> shouldSendTables = TableConstants.getConfigTables(PREFIX);
        List<String> shouldNotSendTables = new ArrayList<String>();
        for (String table : TableConstants.getConfigTablesByVersion(PREFIX).keySet()) {
            shouldSendTables.remove(table);
            shouldNotSendTables.add(table);
        }
        String deployment = isPro ? Constants.DEPLOYMENT_TYPE_PROFESSIONAL : Constants.DEPLOYMENT_TYPE_SERVER;
        Set<String> proTables = TableConstants.getTablesForConsole(PREFIX);
        for (String version : getVersions()) {
            List<String> newTables = versionWithTables.get(version);
            if (newTables != null) {
                for (String table : newTables) {
                    if (isPro || !proTables.contains(table)) {
                        shouldSendTables.add(table);
                        shouldNotSendTables.remove(table);
                    }
                }
            }
            shouldSend(version, shouldSendTables, deployment);
            shouldNotSend(version, shouldNotSendTables, deployment);
        }
        shouldSend("development", shouldSendTables, deployment);
        shouldSend("SNAPSHOT", shouldSendTables, deployment);
    }

    protected List<String> getVersions() {
        List<String> versions = new ArrayList<String>();
        for (int i = 2; i < 5; i++) {
            for (int j = 0; j < 20; j++) {
                for (int k = 0; k < 20; k++) {
                    versions.add(i + "." + j + "." + k);
                }
            }
        }
        return versions;
    }

    protected Map<String, List<String>> getVersionWithTables() {
        Map<String, List<String>> versionWithTables = new HashMap<String, List<String>>();
        for (Map.Entry<String, String> entry : TableConstants.getConfigTablesByVersion(PREFIX).entrySet()) {
            String table = entry.getKey();
            String version = entry.getValue();
            List<String> tables = versionWithTables.get(version);
            if (tables == null) {
                tables = new ArrayList<String>();
            }
            tables.add(table);
            versionWithTables.put(version, tables);
        }
        return versionWithTables;
    }

    protected void shouldSend(String version, List<String> tables, String deployment) {
        checkSend(version, tables, true, deployment);
    }

    protected void shouldNotSend(String version, List<String> tables, String deployment) {
        checkSend(version, tables, false, deployment);
    }

    protected void checkSend(String version, List<String> tables, boolean shouldSend, String deployment) {
        ConfigurationVersionHelper helper = new ConfigurationVersionHelper(PREFIX);
        Node node = new Node();
        node.setSymmetricVersion(version);
        node.setDeploymentType(deployment);
        helper.setTargetNode(node);
        for (String table : tables) {
            String message = "Version " + version + " node " + (shouldSend ? "SHOULD" : "should NOT") + " be sent " + table + " table";
            if (shouldSend) {
                assertTrue(message, helper.shouldSendTable(table));
            } else {
                assertFalse(message, helper.shouldSendTable(table));
            }
        }
    }
}
