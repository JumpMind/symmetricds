package org.jumpmind.symmetric.io.data.writer;

import java.sql.Timestamp;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.symmetric.io.data.ConflictException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.ConflictSetting.DetectInsertConflict;
import org.jumpmind.symmetric.io.data.writer.ConflictSetting.DetectUpdateConflict;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter.LoadStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseWriterConflictResolver implements IDatabaseWriterConflictResolver {

    protected Logger log = LoggerFactory.getLogger(getClass());

    protected boolean autoRegister;

    public void needsResolved(DatabaseWriter writer, DatabaseWriterSettings writerSettings,
            CsvData data, LoadStatus loadStatus) {
        DataEventType originalEventType = data.getDataEventType();
        ConflictSetting conflictSetting = writerSettings.getConflictSettings(
                writer.getTargetTable(), writer.getBatch());
        switch (originalEventType) {
            case INSERT:
                switch (conflictSetting.getResolveInsertType()) {
                    case MANUAL:
                        throw new ConflictException(data, writer.getTargetTable(), loadStatus,
                                false);
                    case FALLBACK:
                        performFallbackToUpdate(writer, data,
                                conflictSetting.isResolveChangesOnly());
                        break;
                    case NEWER_WINS:
                        if ((conflictSetting.getDetectInsertType() == DetectInsertConflict.USE_TIMESTAMP && isTimestampNewer(
                                conflictSetting, writer, data))
                                || (conflictSetting.getDetectInsertType() == DetectInsertConflict.USE_VERSION && isVersionNewer(
                                        conflictSetting, writer, data))) {
                            try {
                                performFallbackToUpdate(writer, data, false);
                            } catch (RuntimeException ex) {
                                throw ex;
                            }
                        } else {
                        }
                        break;
                    case IGNORE:
                    default:
                        if (conflictSetting.isResolveRowOnly()) {
                        } else {
                            throw new NotImplementedException();
                        }
                        break;
                }
                break;

            case UPDATE:
                switch (conflictSetting.getResolveUpdateType()) {
                    case MANUAL:
                        throw new ConflictException(data, writer.getTargetTable(), loadStatus,
                                false);
                    case FALLBACK:
                        performFallbackToInsert(writer, data);
                        break;
                    case NEWER_WINS:
                        if ((conflictSetting.getDetectUpdateType() == DetectUpdateConflict.USE_TIMESTAMP && isTimestampNewer(
                                conflictSetting, writer, data))
                                || (conflictSetting.getDetectUpdateType() == DetectUpdateConflict.USE_VERSION && isVersionNewer(
                                        conflictSetting, writer, data))) {
                            performFallbackToUpdate(writer, data,
                                    conflictSetting.isResolveChangesOnly());
                        } else {
                        }
                        break;
                    case IGNORE:
                    default:
                        if (conflictSetting.isResolveRowOnly()) {
                        } else {
                            throw new NotImplementedException();
                        }
                        break;
                }
                break;

            case DELETE:
                switch (conflictSetting.getResolveDeleteType()) {
                    case MANUAL:
                        throw new ConflictException(data, writer.getTargetTable(), loadStatus,
                                false);
                    default:
                    case IGNORE:
                        if (conflictSetting.isResolveRowOnly()) {
                            writer.getStatistics().get(writer.getBatch())
                                    .increment(DataWriterStatisticConstants.MISSINGDELETECOUNT);
                            break;
                        } else {
                            throw new NotImplementedException();
                        }
                }
                break;

            default:
                break;
        }
    }

    protected boolean isTimestampNewer(ConflictSetting conflictSetting, DatabaseWriter writer,
            CsvData data) {
        String columnName = conflictSetting.getDetectExpresssion();
        Table table = writer.getTargetTable();
        String[] pkData = data.getPkData(table);
        Object[] objectValues = writer.getPlatform().getObjectValues(
                writer.getBatch().getBinaryEncoding(), pkData, table.getPrimaryKeyColumns());
        DmlStatement stmt = writer.getPlatform().createDmlStatement(DmlType.FROM, table);
        String sql = stmt.getColumnsSql(new Column[] { table.getColumnWithName(columnName) });
        Timestamp existingTs = writer.getTransaction().queryForObject(sql, Timestamp.class,
                objectValues);
        Map<String, String> newData = data.toColumnNameValuePairs(table, CsvData.ROW_DATA);
        Timestamp loadingTs = Timestamp.valueOf(newData.get(columnName));
        return loadingTs.after(existingTs);
    }

    protected boolean isVersionNewer(ConflictSetting conflictSetting, DatabaseWriter writer,
            CsvData data) {
        String columnName = conflictSetting.getDetectExpresssion();
        Table table = writer.getTargetTable();
        String[] pkData = data.getPkData(table);
        Object[] objectValues = writer.getPlatform().getObjectValues(
                writer.getBatch().getBinaryEncoding(), pkData, table.getPrimaryKeyColumns());
        DmlStatement stmt = writer.getPlatform().createDmlStatement(DmlType.FROM, table);
        String sql = stmt.getColumnsSql(new Column[] { table.getColumnWithName(columnName) });
        Long existingVersion = writer.getTransaction()
                .queryForObject(sql, Long.class, objectValues);
        Map<String, String> newData = data.toColumnNameValuePairs(table, CsvData.ROW_DATA);
        Long loadingVersion = Long.valueOf(newData.get(columnName));
        return loadingVersion > existingVersion;
    }

    protected void performFallbackToUpdate(DatabaseWriter writer, CsvData data,
            boolean fallbackChanges) {
        LoadStatus loadStatus = writer.update(data, fallbackChanges, false);
        if (loadStatus != LoadStatus.SUCCESS) {
            throw new ConflictException(data, writer.getTargetTable(), loadStatus, true);
        } else {
            writer.getStatistics().get(writer.getBatch())
                    .increment(DataWriterStatisticConstants.FALLBACKUPDATECOUNT);
        }
    }

    protected void performFallbackToInsert(DatabaseWriter writer, CsvData csvData) {
        LoadStatus loadStatus = writer.insert(csvData);
        if (loadStatus != LoadStatus.SUCCESS) {
            throw new ConflictException(csvData, writer.getTargetTable(), loadStatus, true);
        } else {
            writer.getStatistics().get(writer.getBatch())
                    .increment(DataWriterStatisticConstants.FALLBACKINSERTCOUNT);
        }
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

}
