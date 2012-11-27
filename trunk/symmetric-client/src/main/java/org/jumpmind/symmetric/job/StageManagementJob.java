package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class StageManagementJob extends AbstractJob {

    private IStagingManager stagingManager;

    public StageManagementJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler,
            IStagingManager stagingManager) {
        super("job.stage.management", true, engine.getParameterService().is(
                "start.stage.management.job"), engine, taskScheduler);
        this.stagingManager = stagingManager;
    }

    public String getClusterLockName() {
        return ClusterConstants.STAGE_MANAGEMENT;
    }

    public boolean isClusterable() {
        return true;
    }

    @Override
    void doJob(boolean force) throws Exception {
        if (stagingManager != null) {
            stagingManager.clean();
        }
        
        IOutgoingBatchService outgoingBatchService = engine.getOutgoingBatchService();
        
    }

}
