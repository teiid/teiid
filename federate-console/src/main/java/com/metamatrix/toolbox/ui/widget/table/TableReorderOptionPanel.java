/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Arrays;
import java.util.Iterator;

import javax.swing.Box;
import javax.swing.DefaultListModel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.TableColumn;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.ListWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.util.WidgetUtilities;

/**
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class TableReorderOptionPanel extends JPanel {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private final TableWidget table;
    private ListWidget list = null;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public TableReorderOptionPanel(final TableWidget table) {
        this.table = table;
        initializeTableReorderOptionPanel();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public Object[] getColumns() {
        return ((DefaultListModel)list.getModel()).toArray();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    protected void initializeTableReorderOptionPanel() {
        final ButtonWidget upButton = new ButtonWidget("Up");
        final ButtonWidget downButton = new ButtonWidget("Down");
        WidgetUtilities.equalizeSizeConstraints(Arrays.asList(new ButtonWidget[] {upButton, downButton}));
        final DefaultListModel model = new DefaultListModel();
        final Iterator iterator = table.getEnhancedColumnModel().getHiddenAndShownColumns().iterator();
        while (iterator.hasNext()) {
            model.addElement(((TableColumn)iterator.next()).getIdentifier());
        }
        list = new ListWidget(model);
        list.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        updateUpDownEnabled(list, upButton, downButton);

        // Add controllers to components        
        list.addListSelectionListener(new ListSelectionListener() {
            public void valueChanged(final ListSelectionEvent event) {
                updateUpDownEnabled(list, upButton, downButton);
            }
        });
        upButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                int ndx = list.getSelectedIndex();
                final Object val = model.remove(ndx--);
                model.add(ndx, val);
                list.setSelectedIndex(ndx);
            }
        });
        downButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                int ndx = list.getSelectedIndex();
                final Object val = model.remove(ndx++);
                model.add(ndx, val);
                list.setSelectedIndex(ndx);
            }
        });

        setLayout(new BorderLayout());
        final Box box = Box.createVerticalBox();
        box.add(upButton);
        box.add(downButton);
        add(box, BorderLayout.EAST);
        add(new JScrollPane(list), BorderLayout.CENTER);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setSelectedColumn(final TableColumn column) {
        list.setSelectedValue(column.getIdentifier(), true);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    protected void updateUpDownEnabled(final ListWidget list, final ButtonWidget upButton, final ButtonWidget downButton) {
        final int ndx = list.getSelectedIndex();
        upButton.setEnabled(ndx > 0);
        downButton.setEnabled(ndx >= 0  &&  ndx < list.getModel().getSize() - 1);
    }
}
