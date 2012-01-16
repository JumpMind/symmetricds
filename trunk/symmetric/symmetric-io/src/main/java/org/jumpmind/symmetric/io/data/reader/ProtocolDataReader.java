package org.jumpmind.symmetric.io.data.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.exception.IoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.symmetric.io.IoResource;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.Statistics;

public class ProtocolDataReader implements IDataReader {

    protected Logger log = LoggerFactory.getLogger(getClass());

    protected IoResource ioResource;
    protected Reader reader;
    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();
    protected CsvReader csvReader;
    protected DataContext<? extends IDataReader, ? extends IDataWriter> context;
    protected Map<String, Table> tables = new HashMap<String, Table>();
    protected Object next;
    protected Batch batch;
    protected Table table;
    protected String channelId;
    protected String sourceNodeId;
    protected BinaryEncoding binaryEncoding;

    public ProtocolDataReader(StringBuilder input) {
        this(new BufferedReader(new StringReader(input.toString())));
    }

    public ProtocolDataReader(InputStream is) {
        this(toReader(is));
    }

    public ProtocolDataReader(IoResource ioResource) {
        this.ioResource = ioResource;
    }

    protected static Reader toReader(InputStream is) {
        try {
            return new BufferedReader(new InputStreamReader(is, "UTF-8"));
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    public ProtocolDataReader(String input) {
        this(new BufferedReader(new StringReader(input)));
    }

    public ProtocolDataReader(Reader reader) {
        this.reader = reader;
    }

    public ProtocolDataReader(File file) {
        try {
            FileInputStream fis = new FileInputStream(file);
            InputStreamReader in = new InputStreamReader(fis, "UTF-8");
            this.reader = new BufferedReader(in);
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    public <R extends IDataReader, W extends IDataWriter> void open(DataContext<R, W> context) {
        if (this.ioResource != null && this.reader == null) {
            this.reader = toReader(this.ioResource.open());
        }
        this.context = context;
        this.csvReader = CsvUtils.getCsvReader(reader);
        this.next = readNext();
    }

    protected Object readNext() {
        try {
            Set<String> keys = null;
            String schemaName = null;
            String catalogName = null;
            String[] parsedOldData = null;
            long bytesRead = 0;
            while (csvReader.readRecord()) {
                if (log.isDebugEnabled()) {
                    log.debug(csvReader.getRawRecord());
                }
                String[] tokens = csvReader.getValues();
                if (batch == null) {
                    bytesRead += csvReader.getRawRecord().length();
                } else {
                    statistics.get(batch).increment(DataReaderStatistics.READ_BYTE_COUNT,
                            csvReader.getRawRecord().length() + bytesRead);
                    bytesRead = 0;
                }
                if (tokens[0].equals(CsvConstants.BATCH)) {
                    Batch batch = new Batch(Long.parseLong(tokens[1]), channelId, binaryEncoding,
                            sourceNodeId);
                    statistics.put(batch, new DataReaderStatistics());
                    return batch;
                } else if (tokens[0].equals(CsvConstants.NODEID)) {
                    this.sourceNodeId = tokens[1];
                } else if (tokens[0].equals(CsvConstants.BINARY)) {
                    this.binaryEncoding = BinaryEncoding.valueOf(tokens[1]);
                } else if (tokens[0].equals(CsvConstants.CHANNEL)) {
                    this.channelId = tokens[1];
                } else if (tokens[0].equals(CsvConstants.SCHEMA)) {
                    schemaName = StringUtils.isBlank(tokens[1]) ? null : tokens[1];
                } else if (tokens[0].equals(CsvConstants.CATALOG)) {
                    catalogName = StringUtils.isBlank(tokens[1]) ? null : tokens[1];
                } else if (tokens[0].equals(CsvConstants.TABLE)) {
                    String tableName = tokens[1];
                    table = tables.get(Table.getFullyQualifiedTableName(catalogName, schemaName,
                            tableName));
                    if (table != null) {
                        return table;
                    } else {
                        table = new Table(catalogName, schemaName, tableName);
                    }
                } else if (tokens[0].equals(CsvConstants.KEYS)) {
                    if (keys == null) {
                        keys = new HashSet<String>(tokens.length);
                    }
                    for (int i = 1; i < tokens.length; i++) {
                        keys.add(tokens[i]);
                    }
                } else if (tokens[0].equals(CsvConstants.COLUMNS)) {
                    for (int i = 1; i < tokens.length; i++) {
                        Column column = new Column(tokens[i], keys != null
                                && keys.contains(tokens[i]));
                        table.addColumn(column);
                    }
                    tables.put(table.getFullyQualifiedTableName(), table);
                    return table;
                } else if (tokens[0].equals(CsvConstants.COMMIT)) {
                    return null;
                } else if (tokens[0].equals(CsvConstants.INSERT)) {
                    CsvData data = new CsvData();
                    data.setDataEventType(DataEventType.INSERT);
                    data.putParsedData(CsvData.ROW_DATA,
                            Arrays.copyOfRange(tokens, 1, tokens.length));
                    return data;
                } else if (tokens[0].equals(CsvConstants.OLD)) {
                    parsedOldData = Arrays.copyOfRange(tokens, 1, tokens.length);
                } else if (tokens[0].equals(CsvConstants.UPDATE)) {
                    CsvData data = new CsvData();
                    data.setDataEventType(DataEventType.UPDATE);
                    data.putParsedData(CsvData.ROW_DATA,
                            Arrays.copyOfRange(tokens, 1, table.getColumnCount() + 1));
                    data.putParsedData(CsvData.PK_DATA,
                            Arrays.copyOfRange(tokens, table.getColumnCount() + 1, tokens.length));
                    data.putParsedData(CsvData.OLD_DATA, parsedOldData);
                    return data;
                } else if (tokens[0].equals(CsvConstants.DELETE)) {
                    CsvData data = new CsvData();
                    data.setDataEventType(DataEventType.DELETE);
                    data.putParsedData(CsvData.PK_DATA,
                            Arrays.copyOfRange(tokens, 1, tokens.length));
                    data.putParsedData(CsvData.OLD_DATA, parsedOldData);
                    return data;
                } else if (tokens[0].equals(CsvConstants.SQL)) {
                    CsvData data = new CsvData();
                    data.setDataEventType(DataEventType.SQL);
                    data.putCsvData(CsvData.ROW_DATA, tokens[1]);
                    return data;
                } else if (tokens[0].equals(CsvConstants.BSH)) {
                    CsvData data = new CsvData();
                    data.setDataEventType(DataEventType.BSH);
                    data.putCsvData(CsvData.ROW_DATA, tokens[1]);
                    return data;
                } else if (tokens[0].equals(CsvConstants.CREATE)) {
                    CsvData data = new CsvData();
                    data.setDataEventType(DataEventType.CREATE);
                    data.putCsvData(CsvData.ROW_DATA, tokens[1]);
                    return data;
                } else {
                    log.info("Unable to handle unknown csv values: " + Arrays.toString(tokens));
                }
            }
        } catch (IOException ex) {
            throw new IoException(ex);
        }

        return null;

    }

    public Batch nextBatch() {
        if (next instanceof Batch) {
            this.batch = (Batch) next;
            next = null;
            return batch;
        } else {
            do {
                next = readNext();
                if (next instanceof Batch) {
                    this.batch = (Batch) next;
                    next = null;
                    return batch;
                }
            } while (next != null);
        }
        return null;
    }

    public Table nextTable() {
        if (next instanceof Table) {
            this.table = (Table) next;
            next = null;
            return table;
        } else {
            do {
                next = readNext();
                if (next instanceof Table) {
                    this.table = (Table) next;
                    next = null;
                    return table;
                }
            } while (next != null && !(next instanceof Batch));
        }
        return null;
    }

    public CsvData nextData() {
        if (next instanceof CsvData) {
            CsvData data = (CsvData) next;
            next = null;
            return data;
        } else {
            do {
                next = readNext();
                if (next instanceof CsvData) {
                    CsvData data = (CsvData) next;
                    next = null;
                    return data;
                }
            } while (next != null && !(next instanceof Batch) && !(next instanceof Table));
        }
        return null;
    }

    public void close() {
        if (csvReader != null) {
            csvReader.close();
        }
        
    }

    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

}
