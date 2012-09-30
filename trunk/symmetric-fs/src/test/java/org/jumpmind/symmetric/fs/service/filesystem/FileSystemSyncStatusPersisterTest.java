package org.jumpmind.symmetric.fs.service.filesystem;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.jumpmind.symmetric.fs.client.SyncStatus;
import org.jumpmind.symmetric.fs.config.DirectorySpec;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.config.NodeDirectoryKey;
import org.jumpmind.symmetric.fs.config.SyncConfig;
import org.jumpmind.symmetric.fs.service.filesystem.FileSystemSyncStatusPersister;
import org.jumpmind.symmetric.fs.track.DirectorySpecSnapshot;
import org.junit.Assert;
import org.junit.Test;

public class FileSystemSyncStatusPersisterTest {

    @Test
    public void testSave() throws Exception {
        final File dir = new File("target/sync_status");
        FileUtils.deleteDirectory(dir);
        dir.mkdirs();
        FileSystemSyncStatusPersister persister = new FileSystemSyncStatusPersister(dir.getAbsolutePath());
        Node node = new Node("12345", "clientgroup", "http://10.2.34.11/fsync", "DFR#$#3223S#D%%");
        SyncStatus status = new SyncStatus(node, new SyncConfig());
        DirectorySpec spec = new DirectorySpec(true, null, new String[] {".svn"});
        String directory = "/opt/send";
        NodeDirectoryKey key = new NodeDirectoryKey(node, directory);
        status.setDirectorySpecSnapshot(new DirectorySpecSnapshot(node, directory, spec));
        persister.save(status, key);
        
        SyncStatus newStatus = persister.get(SyncStatus.class, key);
        Assert.assertNotNull(newStatus);
        Assert.assertNotNull(newStatus.getNode());
        Assert.assertNotNull(newStatus.getStage());
        Assert.assertEquals(status.getStage(), newStatus.getStage());
        
        File expectedFile = new File(dir, persister.buildFileNameFor(key));
        Assert.assertTrue(expectedFile.exists());
        
    }
}
