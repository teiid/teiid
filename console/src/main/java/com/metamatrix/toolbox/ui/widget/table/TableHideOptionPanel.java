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
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Iterator;
import java.util.List;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.table.TableColumn;

import com.metamatrix.toolbox.ui.widget.TableWidget;

/**
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class TableHideOptionPanel extends AbstractTableOptionPanel {
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public TableHideOptionPanel(final TableWidget table) {
        super(table);
        
        initializeTableHideOptionPanel();
        
        // To fix the first/only entry when no cols are yet hidden
        if ( getColumnCount() == 1 && getTable().getHiddenColumns() == null ) {
            handleDeleteAction( getLastPanel() );
        }

    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    protected JComponent addColumnPanel(final int index) {
        AbstractTableOptionPanel.ColumnPanel pnl;
        JComboBox cbx;
        
        // disable checkboxes and AND button of the previous column panel (if exists)
        if (index > 0) {
            pnl = (AbstractTableOptionPanel.ColumnPanel)getColumnPanel(index - 1);
            cbx = pnl.getColumnsComboBox();
            cbx.setEnabled(false);
            pnl.getAndButton().setEnabled(false);
        }
        
        // added column panel
        pnl = (AbstractTableOptionPanel.ColumnPanel)super.addColumnPanel(index);
        pnl.getDeleteButton().addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent theEvent) {
            }
        });
        
        cbx = pnl.getColumnsComboBox();
        
        // add listeners to current panel
        // remember to delete listeners also
        
        // for each of previous column panels, don't include column in new panel
        for (int numColumns = getColumnCount(), i = 0; i < numColumns; i++) {
            cbx.removeItem(getColumnPanelSelection(i));
        }
        
        return pnl;
    }

    protected void handleAndAction(ColumnPanel thePanel) {
        super.handleAndAction(thePanel);
    }
    
    protected void handleColumnSelected(ColumnPanel thePanel) {
        super.handleColumnSelected(thePanel);
    }

    protected void handleDeleteAction(ColumnPanel thePanel) {
        super.handleDeleteAction(thePanel);        
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Remember to restrict accumulator panel from hiding all columns.
    @since Golden Gate
    */
    protected void initializeTableHideOptionPanel() {
        JComponent colPanel = null;
        final List hiddenCols = getTable().getHiddenColumns();
        if (hiddenCols != null) {
            final Iterator iterator = hiddenCols.iterator();
            for (int col = 0;  iterator.hasNext();  ++col) {
                colPanel = addColumnPanel(col);
                ((JComboBox)colPanel.getComponent(1)).setSelectedItem(((TableColumn)iterator.next()).getIdentifier());
            }
        } 

        initializeColumnsPanel(colPanel);
    }
   
}
