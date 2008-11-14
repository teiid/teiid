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

import java.util.Arrays;
import java.util.Vector;

public class EntitlementsTableModel
        extends com.metamatrix.toolbox.ui.widget.table.DefaultTableModel {
    public final static int  ENTITLEMENT_COL_NUM = 0;
    public final static String ENTITLEMENT_NAME = "Role Name";  //$NON-NLS-1$
    public final static int VDB_COL_NUM = 1;
    public final static String VDB_NAME = "VDB Name"; //$NON-NLS-1$
    public final static int VDB_VERS_COL_NUM = 2;
    public final static String VDB_VERSION = "VDB Version"; //$NON-NLS-1$

    public EntitlementsTableModel(EntitlementsTableRowData[] rows) {
        //NOTE-- columns must be given in order as defined above
        super(new Vector(Arrays.asList(new String[] {ENTITLEMENT_NAME, VDB_NAME, VDB_VERSION})), 0);
        init(rows);
    }

    public Class getColumnClass(int col) {
        Class cls;
        if (col == VDB_VERS_COL_NUM) {
            cls = Integer.class;
        } else {
            cls = String.class;
        }
        return cls;
    }

    public void init(EntitlementsTableRowData[] rows) {
        this.setNumRows(rows.length);
        for (int i = 0; i < rows.length; i++) {
            if (rows[i] != null){
                setValueAt(rows[i].getEntitlementName(), i, ENTITLEMENT_COL_NUM);
                setValueAt(rows[i].getVDBName(), i, VDB_COL_NUM);
                int vdbVersion = rows[i].getVDBVersion();
                if (vdbVersion >= 0) {
                    setValueAt(new Integer(rows[i].getVDBVersion()), i, VDB_VERS_COL_NUM);
                } else {
                    setValueAt(null, i, VDB_VERS_COL_NUM);
                }
            }
        }
    }
}
