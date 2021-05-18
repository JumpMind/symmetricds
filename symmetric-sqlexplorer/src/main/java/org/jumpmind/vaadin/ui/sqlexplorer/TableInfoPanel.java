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
package org.jumpmind.vaadin.ui.sqlexplorer;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.symmetric.io.data.DbExport;
import org.jumpmind.symmetric.io.data.DbExport.Format;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.jumpmind.vaadin.ui.sqlexplorer.SqlRunner.ISqlRunnerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vaadin.aceeditor.AceEditor;
import org.vaadin.aceeditor.AceMode;

import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.ComponentUtil;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.progressbar.ProgressBar;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;

public class TableInfoPanel extends VerticalLayout implements IInfoPanel {

    protected static final Logger log = LoggerFactory.getLogger(TableInfoPanel.class);

    private static final long serialVersionUID = 1L;
    
    SqlExplorer explorer;

    //TabSheet tabSheet;
    
    String selectedCaption;
    
    public TableInfoPanel(final org.jumpmind.db.model.Table table, final String user, final IDb db,
            final Settings settings, String selectedTabCaption) {
        this(table, user, db, settings, null, selectedTabCaption);
    }

    public TableInfoPanel(final org.jumpmind.db.model.Table table, final String user, final IDb db,
            final Settings settings, SqlExplorer explorer, String selectedTabCaption) {
        this.explorer = explorer;
        
        setSizeFull();

        /*tabSheet = CommonUiUtils.createTabSheet();
        tabSheet.addSelectedTabChangeListener(new SelectedTabChangeListener() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public void selectedTabChange(SelectedTabChangeEvent event) {
                selectedCaption = tabSheet.getTab(tabSheet.getSelectedTab()).getCaption();
                
                if (tabSheet.getSelectedTab() instanceof AbstractLayout) {
                    AbstractLayout layout = (AbstractLayout) tabSheet.getSelectedTab();
                    if (selectedCaption.equals("Data") && layout.getData() != null && layout.getData().equals(true)) {
                        refreshData(table, user, db, settings, false);
                    } else if (layout.getData() != null && layout.getData() instanceof AbstractMetaDataGridCreator) {
                        populate((VerticalLayout) layout);
                    }
                } else if (tabSheet.getSelectedTab() instanceof AceEditor &&
                        ((AceEditor) tabSheet.getSelectedTab()).getData().equals(true)) {
                    populateSource(table, db, (AceEditor) tabSheet.getSelectedTab());
                }
            }
        });
        add(tabSheet);

        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) db.getPlatform().getSqlTemplate();

        tabSheet.addTab(create(new ColumnMetaDataTableCreator(sqlTemplate, table, settings)),
                "Columns");
        tabSheet.addTab(create(new PrimaryKeyMetaDataTableCreator(sqlTemplate, table, settings)),
                "Primary Keys");
        tabSheet.addTab(create(new IndexMetaDataTableCreator(sqlTemplate, table, settings)),
                "Indexes");
        if (db.getPlatform().getDatabaseInfo().isForeignKeysSupported()) {
            tabSheet.addTab(create(new ImportedKeysMetaDataTableCreator(sqlTemplate, table,
                    settings)), "Imported Keys");
            tabSheet.addTab(create(new ExportedKeysMetaDataTableCreator(sqlTemplate, table,
                    settings)), "Exported Keys");
        }
        
        refreshData(table, user, db, settings, true);
        
        AceEditor editor = new AceEditor();
        ComponentUtil.setData(editor, "data", true);
        tabSheet.addTab(editor, "Source");
        
        Iterator<Component> i = tabSheet.iterator();
        while (i.hasNext()) {
            Component component = i.next();
            Tab tab = tabSheet.getTab(component);
            if (tab.getCaption().equals(selectedTabCaption)) {
                tabSheet.setSelectedTab(component);
                break;
            }            
        }*/
        
    }
    
    public String getSelectedTabCaption() {
        return selectedCaption;
    }

    protected void refreshData(final org.jumpmind.db.model.Table table, final String user, final IDb db,
            final Settings settings, boolean isInit) {
        
        if (!isInit) {
            //tabSheet.removeTab(tabSheet.getTab(1));
        }
        
        IDatabasePlatform platform = db.getPlatform();
        DmlStatement dml = platform.createDmlStatement(DmlType.SELECT_ALL, table, null);

        final HorizontalLayout executingLayout = new HorizontalLayout();
        executingLayout.setSizeFull();
        final ProgressBar p = new ProgressBar();
        p.setIndeterminate(true);
        executingLayout.add(p);
        ComponentUtil.setData(executingLayout, "isInit", isInit);
        //tabSheet.addTab(executingLayout, "Data", isInit ? null : VaadinIcon.SPINNER, 1);
        if (!isInit) {
            //tabSheet.setSelectedTab(executingLayout);
        }

        SqlRunner runner = new SqlRunner(dml.getSql(), false, user, db, settings, explorer,
                new ISqlRunnerListener() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void writeSql(String sql) {
                        //explorer.openQueryWindow(db).appendSql(sql);
                    }

                    @Override
                    public void reExecute(String sql) {
                        refreshData(table, user, db, settings, false);
                    }

                    @Override
                    public void finished(final VaadinIcon icon, final List<Component> results,
                            long executionTimeInMs, boolean transactionStarted,
                            boolean transactionEnded) {
                        /*TableInfoPanel.this.getUI().access(new Runnable() {

                            @Override
                            public void run() {
                                tabSheet.remove(executingLayout);
                                VerticalLayout layout = new VerticalLayout();
                                layout.setMargin(true);
                                layout.setSizeFull();
                                if (results.size() > 0) {
                                    layout.add(results.get(0));
                                }
                                tabSheet.addTab(layout, "Data", null, 1);
                                tabSheet.setSelectedTab(layout);
                            }
                        });*/
                    }
                });
        runner.setShowSqlOnResults(false);
        runner.setLogAtDebug(true);
        if (!isInit) {
            runner.start();
        }
        
    }

    protected VerticalLayout create(AbstractMetaDataGridCreator creator) {
        VerticalLayout layout = new VerticalLayout();
        layout.setMargin(false);
        layout.setSizeFull();
        ComponentUtil.setData(layout, "creator", creator);
        return layout;
    }
    
    protected void populate(VerticalLayout layout) {
        AbstractMetaDataGridCreator creator = (AbstractMetaDataGridCreator) ComponentUtil.getData(layout, "creator");
        Grid<List<Object>> grid = creator.create();
        layout.add(grid);
        layout.expand(grid);
        ComponentUtil.setData(layout, "creator", null);
    }
    
    protected void populateSource(org.jumpmind.db.model.Table table, IDb db, AceEditor oldTab) {
        try {
            //tabSheet.removeTab(tabSheet.getTab(oldTab));
            DbExport export = new DbExport(db.getPlatform());
            export.setNoCreateInfo(false);
            export.setNoData(true);
            export.setCatalog(table.getCatalog());
            export.setSchema(table.getSchema());
            export.setFormat(Format.SQL);
            /*AceEditor editor = CommonUiUtils.createAceEditor();
            editor.setMode(AceMode.sql);
            editor.setValue(export.exportTables(new org.jumpmind.db.model.Table[] { table }));
            ComponentUtil.setData(editor, "data", false);
            tabSheet.addTab(editor, "Source");
            tabSheet.setSelectedTab(editor);*/
        } catch (/*IO*/Exception e) {
            log.warn("Failed to export the create information", e);
        }
    }

    @Override
    public void selected() {
    }

    @Override
    public void unselected() {
    }
}
