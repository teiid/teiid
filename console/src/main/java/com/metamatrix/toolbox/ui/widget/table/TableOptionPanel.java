/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.table;

// System imports
import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.swing.JTabbedPane;

import com.metamatrix.toolbox.ui.widget.ConfigurationPanel;
import com.metamatrix.toolbox.ui.widget.TableWidget;

/**
@since Golden Gate
@version Golden Gate
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class TableOptionPanel extends ConfigurationPanel
implements TableConstants {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private DefaultTableHeader hdr;
    private TableSortOptionPanel sortPanel;
    private TableHideOptionPanel hidePanel;
    private TableReorderOptionPanel reorderPanel;
     
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */ 
    public TableOptionPanel(final DefaultTableHeader hdr) {
        this.hdr = hdr;
        initializeTableOptionPanel();
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */ 
    public AbstractTableFilterOptionPanel getFilterPanel() {
        return hdr.getFilterOptionPanel();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */ 
    protected TableHideOptionPanel getHidePanel() {
        return hidePanel;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */ 
    protected TableReorderOptionPanel getReorderPanel() {
        return reorderPanel;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */ 
    protected TableSortOptionPanel getSortPanel() {
        return sortPanel;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */ 
    public DefaultTableHeader getTableHeader() {
        return hdr;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */ 
    protected void initializeTableOptionPanel() {
        final TableWidget table = (TableWidget)hdr.getTable();
        if (table.isSortable()) {
            sortPanel = new TableSortOptionPanel(table);
        }
        hidePanel = new TableHideOptionPanel(table);
        if (hdr.getReorderingAllowed()) {
            reorderPanel = new TableReorderOptionPanel(table);
        }
        final List tabNames = new ArrayList();
        populateTabNames(tabNames);
        final List tabContents = new ArrayList();
        populateTabContents(tabContents);
        final JTabbedPane tabs = new JTabbedPane();
        final Iterator nameIterator = tabNames.iterator();
        final Iterator panelIterator = tabContents.iterator();
        while (nameIterator.hasNext()) {
            tabs.addTab((String)nameIterator.next(), (Component)panelIterator.next());
        }
        setContent(tabs);
        addApplyActionListener(new ActionListener() {
            final EnhancedTableColumnModel colModel = table.getEnhancedColumnModel();
            public void actionPerformed(final ActionEvent event) {
                EnhancedTableColumn col;
                // Update sort changes
                if (sortPanel != null) {
                    colModel.setColumnsNotSorted();
                    for (int ndx = 0;  ndx < sortPanel.getColumnCount();  ++ndx) {
                        col = sortPanel.getColumn(ndx);
                        if (col != null) {
                            if (sortPanel.isColumnSortedAscending(ndx)) {
                                colModel.setColumnSortedAscending(col,
                                    KEEP_CURRENT_COLUMN_SORT_ORDER);
                            } else {
                                colModel.setColumnSortedDescending(col,
                                    KEEP_CURRENT_COLUMN_SORT_ORDER);
                            }
                        }
                    }
                }
                // Update hide changes
                table.setColumnsHidden(false);
                for (int ndx = 0;  ndx < hidePanel.getColumnCount();  ++ndx) {
                    col = hidePanel.getColumn(ndx);
                    if (col != null  &&  !col.isHidden()) {
                        colModel.setColumnHidden(col, true);
                    }
                }
                table.sizeColumnsToFitContainer(-1);
                // Update filter changes
                final AbstractTableFilterOptionPanel filterPanel = hdr.getFilterOptionPanel();
                if (filterPanel != null) {
                    table.clearFilters();
                    TableFilter filter;
                    for (int ndx = 0;  ndx < filterPanel.getFilterCount();  ++ndx) {
                        filter = filterPanel.getFilter(ndx);
                        if (filter != null  &&  !table.isFilteredBy(filter)) {
                            table.addFilter(filter);
                        }
                    }
                }
                // Update order changes
                if (reorderPanel != null) {
                    hdr.reorderColumns(reorderPanel);
                }
                // Repaint to see changes
                hdr.repaint();
                table.repaint();
            }
        });
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */ 
    protected void populateTabContents(final List tabContents) {
        final AbstractTableFilterOptionPanel filterPanel = hdr.getFilterOptionPanel();
        if (filterPanel != null) {
            tabContents.add(filterPanel);
        }
        if (sortPanel != null) {
            tabContents.add(sortPanel);
        }
        tabContents.add(hidePanel);
        if (reorderPanel != null) {
            tabContents.add(reorderPanel);
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */ 
    protected void populateTabNames(final List tabNames) {
        if (hdr.getFilterOptionPanel() != null) {
            tabNames.add("Filter Rows");
        }
        if (sortPanel != null) {
            tabNames.add("Sort Rows");
        }
        tabNames.add("Hide Columns");
        if (reorderPanel != null) {
            tabNames.add("Reorder Columns");
        }
    }
}
