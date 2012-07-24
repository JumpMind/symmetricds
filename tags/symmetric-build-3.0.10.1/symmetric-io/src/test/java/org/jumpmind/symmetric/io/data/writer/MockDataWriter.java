package org.jumpmind.symmetric.io.data.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.Statistics;

public class MockDataWriter implements IDataWriter {

    protected boolean closeCalled = false;

    protected Map<String, List<CsvData>> writtenDatas = new HashMap<String, List<CsvData>>();

    protected Table currentTable;
    
    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();

    public void open(DataContext context) {
    }

    public void close() {
        closeCalled = true;
    }

    public void start(Batch batch) {
        statistics.put(batch, new Statistics());
    }

    public boolean start(Table table) {
        this.currentTable = table;
        if (!writtenDatas.containsKey(table.getFullyQualifiedTableName())) {
            writtenDatas.put(table.getFullyQualifiedTableName(), new ArrayList<CsvData>());
        }
        return true;
    }

    public void write(CsvData data) {
        writtenDatas.get(this.currentTable.getFullyQualifiedTableName()).add(data);
    }

    public void end(Table table) {
    }

    public void end(Batch batch, boolean inError) {
    }
    
    protected void reset() {
        writtenDatas.clear();
    }
    
    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

}
