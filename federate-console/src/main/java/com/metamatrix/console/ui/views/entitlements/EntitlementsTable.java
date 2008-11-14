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

package com.metamatrix.console.ui.views.entitlements;


import javax.swing.ListSelectionModel;

import com.metamatrix.console.ui.views.DefaultConsoleTableComparator;

import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;

public class EntitlementsTable extends TableWidget {
    public EntitlementsTable(EntitlementsTableModel model) {
        super(model, true);
        init();
    }

    private void init() {
        getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        this.setEditable(false);
        this.setComparator(DefaultConsoleTableComparator.getInstance());
        this.getTableHeader().setReorderingAllowed(false);
        EnhancedTableColumn nameColumn = (EnhancedTableColumn)this.getColumn(
                EntitlementsTableModel.ENTITLEMENT_NAME);
        this.setColumnSortedAscending(nameColumn, false);
    }
}
