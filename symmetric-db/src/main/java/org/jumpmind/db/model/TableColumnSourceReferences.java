package org.jumpmind.db.model;

import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;

/***
 * Holds array of column references (from source to target) based on matching names. Target table columns are a priority. Unreferenced source columns are
 * excluded. The searchKey contains table names and helps find this object in a map.
 */
public class TableColumnSourceReferences extends ArrayList<TableColumnSourceReferences.ColumnSourceReferenceEntry> {
    private static final long serialVersionUID = 1L;
    private String searchKey;

    /***
     * Builds array of column references based on matching names
     */
    public TableColumnSourceReferences(Table sourceTable, Table targetTable) {
        super(sourceTable.getColumns().length);
        this.searchKey = generateSearchKey(sourceTable, targetTable);
        Column[] sourceColumns = sourceTable.getColumns();
        Column[] targetColumns = targetTable.getColumns();
        for (int targetColumnNo = 0; targetColumnNo < targetColumns.length; targetColumnNo++) {
            Column targetColumn = targetColumns[targetColumnNo];
            for (int sourceColumnNo = 0; sourceColumnNo < sourceColumns.length; sourceColumnNo++) {
                Column sourceColumn = sourceColumns[sourceColumnNo];
                if (StringUtils.equalsIgnoreCase(sourceColumn.getName(), targetColumn.getName())) {
                    this.add(new ColumnSourceReferenceEntry(sourceColumnNo, targetColumnNo));
                    break;
                }
            }
        }
    }

    /***
     * Builds key for storing/searching this object in a map
     */
    public static String generateSearchKey(Table sourceTable, Table targetTable) {
        return sourceTable.getFullyQualifiedTableName() + targetTable.getFullyQualifiedTableName();
    }

    public String getSearchKey() {
        return this.searchKey;
    }

    /***
     * Column mappings to move data efficiently. Used to copy data from source to target.
     */
    public record ColumnSourceReferenceEntry(int sourceColumnNo, int targetColumnNo) {
    }
}
