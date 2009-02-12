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

import javax.swing.JComboBox;
import javax.swing.JComponent;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;

/**
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class TableSortOptionPanel extends AbstractTableOptionPanel {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    private static final String[] ORDERS = new String[] {"Ascending", "Descending"};

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public TableSortOptionPanel(final TableWidget table) {
        super(table);
        initializeTableSortOptionPanel();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    protected void customizeColumnPanel(final JComponent columnPanel, final ButtonWidget andButton,
                                        final ButtonWidget deleteButton) {
        andButton.setText(", then");
        columnPanel.add(new LabelWidget(" in "));
        final JComboBox orderBox = new JComboBox(ORDERS);
        orderBox.setMaximumSize(orderBox.getPreferredSize());
        columnPanel.add(orderBox);
        columnPanel.add(new LabelWidget(" order"));
        deleteButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                final JComponent colsPanel = (JComponent)columnPanel.getParent();
                if (colsPanel.getComponentCount() == 1) {
                    orderBox.setSelectedItem("Ascending");
                } else {
                    colsPanel.remove(columnPanel);
                    colsPanel.revalidate();
                    colsPanel.repaint();
                }
            }
        });
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    protected void initializeTableSortOptionPanel() {
        final TableWidget table = getTable();
        JComponent colPanel = null;
        if (table.getSortedColumnCount() > 0) {
            final Iterator iterator = table.getSortedColumns().iterator();
            EnhancedTableColumn col;
            for (int colNdx = 0;  iterator.hasNext();  ++colNdx) {
                col = (EnhancedTableColumn)iterator.next();
                colPanel = addColumnPanel(colNdx);
                ((JComboBox)colPanel.getComponent(1)).setSelectedItem(col.getIdentifier());
                if (col.isSortedDescending()) {
                    ((JComboBox)colPanel.getComponent(3)).setSelectedItem("Descending");
                }
            }
        }
        initializeColumnsPanel(colPanel);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public boolean isColumnSortedAscending(final int index) {
        return ((JComboBox)getColumnPanel(index).getComponent(3)).getSelectedItem().equals("Ascending");
    }
}
