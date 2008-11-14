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

package com.metamatrix.console.ui.views.extensionsource;

import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;

public class ExtensionSourcesTableModel extends DefaultTableModel {
    public final static int NUM_COLUMNS = 2;
    public final static int SOURCE_NAME_COLUMN = 0;
    public final static int SOURCE_TYPE_COLUMN = 1;
//    public final static int ENABLED_COLUMN = 2;

    //Note-- must be in same order as above:
    public final static String[] COLUMN_HEADERS = {"Module Name", "Module Type"};
                                                   //, "Enabled"};

    public ExtensionSourcesTableModel() {
        super();
        this.setColumnIdentifiers(COLUMN_HEADERS);
    }

    public Class getColumnClass(int column) {
        return String.class;
//        Class cls;
//        if (column == ENABLED_COLUMN) {
//            cls = Boolean.class;
//        } else {
//            cls = String.class;
//        }
//        return cls;
    }
}
