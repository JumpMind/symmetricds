package org.jumpmind.symmetric.route;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.core.JdbcTemplate;

public interface IRoutingContext {
    
    public JdbcTemplate getJdbcTemplate();

    public NodeChannel getChannel();

    public Map<String, OutgoingBatch> getBatchesByNodes();

    public Map<String, OutgoingBatchHistory> getBatchHistoryByNodes();

    public Map<Trigger, Set<Node>> getAvailableNodes();

    public void commit() throws SQLException;
    
    public void rollback();
    
    public void cleanup();
    
}
