package org.jumpmind.symmetric.io.data.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.log.Log;
import org.jumpmind.log.LogFactory;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.transform.AdditiveColumnTransform;
import org.jumpmind.symmetric.io.data.transform.BshColumnTransform;
import org.jumpmind.symmetric.io.data.transform.ConstantColumnTransform;
import org.jumpmind.symmetric.io.data.transform.CopyColumnTransform;
import org.jumpmind.symmetric.io.data.transform.DeleteAction;
import org.jumpmind.symmetric.io.data.transform.IColumnTransform;
import org.jumpmind.symmetric.io.data.transform.ISingleValueColumnTransform;
import org.jumpmind.symmetric.io.data.transform.IdentityColumnTransform;
import org.jumpmind.symmetric.io.data.transform.IgnoreColumnException;
import org.jumpmind.symmetric.io.data.transform.IgnoreRowException;
import org.jumpmind.symmetric.io.data.transform.LookupColumnTransform;
import org.jumpmind.symmetric.io.data.transform.MultiplierColumnTransform;
import org.jumpmind.symmetric.io.data.transform.SubstrColumnTransform;
import org.jumpmind.symmetric.io.data.transform.TransformColumn;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.jumpmind.symmetric.io.data.transform.TransformedData;
import org.jumpmind.symmetric.io.data.transform.VariableColumnTransform;
import org.jumpmind.util.Statistics;

public class TransformWriter implements IDataWriter {

    protected Log log = LogFactory.getLog(getClass());

    protected IDataWriter targetWriter;
    protected TransformPoint transformPoint;
    protected IDatabasePlatform platform;
    protected Map<String, List<TransformTable>> transformsBySourceTable;

    static protected Map<String, IColumnTransform<?>> columnTransforms = new HashMap<String, IColumnTransform<?>>();

    static {
        columnTransforms.put(AdditiveColumnTransform.NAME, new AdditiveColumnTransform());
        columnTransforms.put(BshColumnTransform.NAME, new BshColumnTransform());
        columnTransforms.put(ConstantColumnTransform.NAME, new ConstantColumnTransform());
        columnTransforms.put(CopyColumnTransform.NAME, new CopyColumnTransform());
        columnTransforms.put(IdentityColumnTransform.NAME, new IdentityColumnTransform());
        columnTransforms.put(MultiplierColumnTransform.NAME, new MultiplierColumnTransform());
        columnTransforms.put(SubstrColumnTransform.NAME, new SubstrColumnTransform());
        columnTransforms.put(VariableColumnTransform.NAME, new VariableColumnTransform());
        columnTransforms.put(LookupColumnTransform.NAME, new LookupColumnTransform());
    }

    public static void addColumnTransform(IColumnTransform<?> columnTransform) {
        columnTransforms.put(columnTransform.getName(), columnTransform);
    }

    protected Table sourceTable;
    protected List<TransformTable> activeTransforms;
    protected DataContext<? extends IDataReader, ? extends IDataWriter> context;

    public TransformWriter(IDatabasePlatform platform, TransformPoint transformPoint,
            IDataWriter targetWriter, TransformTable... transforms) {
        this.platform = platform;
        this.transformPoint = transformPoint == null ? TransformPoint.LOAD : transformPoint;
        this.transformsBySourceTable = toMap(transforms);
        this.targetWriter = targetWriter;
    }

    protected Map<String, List<TransformTable>> toMap(TransformTable[] transforms) {
        Map<String, List<TransformTable>> transformsByTable = new HashMap<String, List<TransformTable>>();
        if (transforms != null) {
            for (TransformTable transformTable : transforms) {
                if (transformPoint == transformTable.getTransformPoint()) {
                    String sourceTableName = transformTable.getFullyQualifiedSourceTableName();
                    List<TransformTable> tables = transformsByTable.get(sourceTableName);
                    if (tables == null) {
                        tables = new ArrayList<TransformTable>();
                        transformsByTable.put(sourceTableName, tables);
                    }
                    tables.add(transformTable);
                }
            }
        }
        return transformsByTable;
    }

    public <R extends IDataReader, W extends IDataWriter> void open(DataContext<R, W> context) {
        this.context = context;
        this.targetWriter.open(context);
    }

    public void close() {
        this.targetWriter.close();
    }

    public void start(Batch batch) {
        this.targetWriter.start(batch);
    }

    public boolean start(Table table) {
        activeTransforms = transformsBySourceTable.get(table.getFullyQualifiedTableName());
        if (activeTransforms != null && activeTransforms.size() > 0) {
            this.sourceTable = table;
            return true;
        } else {
            return this.targetWriter.start(table);
        }
    }

    protected boolean isTransformable(DataEventType eventType) {
        return eventType != null
                && (eventType == DataEventType.INSERT || eventType == DataEventType.UPDATE || eventType == DataEventType.DELETE);
    }

    public void write(CsvData data) {
        DataEventType eventType = data.getDataEventType();
        if (activeTransforms != null && activeTransforms.size() > 0 && isTransformable(eventType)) {
            Map<String, String> sourceValues = data.toColumnNameValuePairs(this.sourceTable,
                    CsvData.ROW_DATA);
            Map<String, String> oldSourceValues = data.toColumnNameValuePairs(this.sourceTable,
                    CsvData.OLD_DATA);
            Map<String, String> sourceKeyValues = oldSourceValues.size() > 0 ? oldSourceValues
                    : sourceValues;

            if (data.contains(CsvData.PK_DATA) && oldSourceValues.size() == 0) {
                sourceKeyValues = data.toColumnNameValuePairs(this.sourceTable, CsvData.PK_DATA);
            }

            if (eventType == DataEventType.DELETE) {
                sourceValues = oldSourceValues;
            }

            if (log.isDebugEnabled()) {
                log.debug(
                        "%d transformation(s) started because of %s on %s.  The original row data was: %s",
                        activeTransforms.size(), eventType.toString(),
                        this.sourceTable.getFullyQualifiedTableName(), sourceValues);
            }

            List<TransformedData> dataThatHasBeenTransformed = new ArrayList<TransformedData>();
            for (TransformTable transformation : activeTransforms) {
                dataThatHasBeenTransformed.addAll(transform(eventType, context, transformation,
                        sourceKeyValues, oldSourceValues, sourceValues));
            }

            for (TransformedData transformedData : dataThatHasBeenTransformed) {
                Table table = transformedData.buildTargetTable();
                this.targetWriter.start(table);
                this.targetWriter.write(transformedData.buildTargetCsvData());
                this.targetWriter.end(table);
            }

        } else {
            this.targetWriter.write(data);
        }

    }

    protected List<TransformedData> transform(DataEventType eventType,
            DataContext<? extends IDataReader, ? extends IDataWriter> context,
            TransformTable transformation, Map<String, String> sourceKeyValues,
            Map<String, String> oldSourceValues, Map<String, String> sourceValues) {
        try {
            List<TransformedData> dataToTransform = create(context, eventType, transformation,
                    sourceKeyValues, oldSourceValues, sourceValues);
            List<TransformedData> dataThatHasBeenTransformed = new ArrayList<TransformedData>(
                    dataToTransform.size());
            if (log.isDebugEnabled()) {
                log.debug(
                        "%d target data was created for the %s transformation.  The target table is %s",
                        dataToTransform.size(), transformation.getTransformId(),
                        transformation.getFullyQualifiedTargetTableName());
            }
            int transformNumber = 0;
            for (TransformedData targetData : dataToTransform) {
                transformNumber++;
                if (perform(context, targetData, transformation, sourceValues, oldSourceValues)) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Data has been transformed to a %s for the #%d transform.  The mapped target columns are: %s. The mapped target values are: %s",
                                targetData.getTargetDmlType().toString(), transformNumber,
                                ArrayUtils.toString(targetData.getColumnNames()),
                                ArrayUtils.toString(targetData.getColumnValues()));
                    }
                    dataThatHasBeenTransformed.add(targetData);
                } else {
                    log.debug("Data has not been transformed for the #%d transform",
                            transformNumber);
                }
            }
            return dataThatHasBeenTransformed;
        } catch (IgnoreRowException ex) {
            // ignore this row
            if (log.isDebugEnabled()) {
                log.debug(
                        "Transform indicated that the target row should be ignored with a target key of: %s",
                        "unknown.  Transformation aborted during tranformation of key");
            }
            return new ArrayList<TransformedData>(0);
        }

    }

    protected boolean perform(DataContext<? extends IDataReader, ? extends IDataWriter> context,
            TransformedData data, TransformTable transformation, Map<String, String> sourceValues,
            Map<String, String> oldSourceValues) throws IgnoreRowException {
        boolean persistData = false;
        try {
            for (TransformColumn transformColumn : transformation.getTransformColumns()) {
                if (transformColumn.getSourceColumnName() == null
                        || sourceValues.containsKey(transformColumn.getSourceColumnName())) {
                    IColumnTransform<?> transform = columnTransforms != null ? columnTransforms
                            .get(transformColumn.getTransformType()) : null;
                    if (transform == null || transform instanceof ISingleValueColumnTransform) {
                        try {
                            String value = (String) transformColumn(context, data, transformColumn,
                                    sourceValues, oldSourceValues);
                            data.put(transformColumn, value, false);
                        } catch (IgnoreColumnException e) {
                            // Do nothing. We are ignoring the column
                        }
                    }
                } else {
                    log.warn("Could not find a source column of %s for the transformation: %s",
                            transformColumn.getSourceColumnName(), transformation.getTransformId());
                }
            }

            if (data.getTargetDmlType() != DataEventType.DELETE) {
                if (data.getTargetDmlType() == DataEventType.INSERT
                        && transformation.isUpdateFirst()) {
                    data.setTargetDmlType(DataEventType.UPDATE);
                }
                persistData = true;
            } else {
                // handle the delete action
                DeleteAction deleteAction = transformation.getDeleteAction();
                switch (deleteAction) {
                case DEL_ROW:
                    data.setTargetDmlType(DataEventType.DELETE);
                    persistData = true;
                    break;
                case UPDATE_COL:
                    data.setTargetDmlType(DataEventType.UPDATE);
                    persistData = true;
                    break;
                case NONE:
                default:
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "The %s transformation is not configured to delete row.  Not sending the delete through.",
                                transformation.getTransformId());
                    }
                }
            }
        } catch (IgnoreRowException ex) {
            // ignore this row
            if (log.isDebugEnabled()) {
                log.debug(
                        "Transform indicated that the target row should be ignored with a target key of: %s",
                        ArrayUtils.toString(data.getKeyValues()));
            }
        }
        return persistData;
    }

    protected List<TransformedData> create(
            DataContext<? extends IDataReader, ? extends IDataWriter> context,
            DataEventType dataEventType, TransformTable transformation,
            Map<String, String> sourceKeyValues, Map<String, String> oldSourceValues,
            Map<String, String> sourceValues) throws IgnoreRowException {
        List<TransformedData> datas = new ArrayList<TransformedData>();
        datas.add(new TransformedData(transformation, dataEventType, sourceKeyValues,
                oldSourceValues, sourceValues));
        List<TransformColumn> columns = transformation.getPrimaryKeyColumns();
        if (columns == null || columns.size() == 0) {
            log.error("No primary key defined for the transformation: %s",
                    transformation.getTransformId());
        } else {
            for (TransformColumn transformColumn : columns) {
                List<TransformedData> newDatas = null;
                for (TransformedData data : datas) {
                    try {
                        Object columnValue = transformColumn(context, data, transformColumn,
                                sourceValues, oldSourceValues);
                        if (columnValue instanceof List) {
                            @SuppressWarnings("unchecked")
                            List<String> values = (List<String>) columnValue;
                            if (values.size() > 0) {
                                data.put(transformColumn, values.get(0), true);
                                if (values.size() > 1) {
                                    if (newDatas == null) {
                                        newDatas = new ArrayList<TransformedData>(values.size() - 1);
                                    }
                                    for (int i = 1; i < values.size(); i++) {
                                        TransformedData newData = data.copy();
                                        newData.put(transformColumn, values.get(i), true);
                                        newDatas.add(newData);
                                    }
                                }
                            } else {
                                throw new IgnoreRowException();
                            }
                        } else {
                            data.put(transformColumn, (String) columnValue, true);
                        }
                    } catch (IgnoreColumnException e) {
                        // Do nothing. We are suppose to ignore the column.
                    }
                }
                if (newDatas != null) {
                    datas.addAll(newDatas);
                    newDatas = null;
                }
            }
        }
        return datas;
    }

    protected Object transformColumn(
            DataContext<? extends IDataReader, ? extends IDataWriter> context,
            TransformedData data, TransformColumn transformColumn,
            Map<String, String> sourceValues, Map<String, String> oldSourceValues)
            throws IgnoreRowException, IgnoreColumnException {
        Object returnValue = null;
        String value = transformColumn.getSourceColumnName() != null ? sourceValues
                .get(transformColumn.getSourceColumnName()) : null;
        returnValue = value;
        IColumnTransform<?> transform = columnTransforms != null ? columnTransforms
                .get(transformColumn.getTransformType()) : null;
        if (transform != null) {
            String oldValue = oldSourceValues.get(transformColumn.getSourceColumnName());
            returnValue = transform.transform(platform, context, transformColumn, data,
                    sourceValues, value, oldValue);
        }
        return returnValue;
    }

    public void end(Table table) {
        if (activeTransforms != null && activeTransforms.size() > 0) {
            activeTransforms = null;
        } else {
            this.targetWriter.end(table);
        }

    }

    public void end(Batch batch, boolean inError) {
        this.targetWriter.end(batch, inError);
    }

    public Map<Batch, Statistics> getStatistics() {
        return this.targetWriter.getStatistics();
    }

}
