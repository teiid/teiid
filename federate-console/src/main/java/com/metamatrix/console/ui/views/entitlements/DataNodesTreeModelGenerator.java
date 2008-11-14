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

import com.metamatrix.platform.admin.api.PermissionTreeView;

public class DataNodesTreeModelGenerator {
    private PermissionTreeView treeView = null;
    private boolean canModify = false;
    private boolean usingRoot;
    private DataNodesTreeModel model = null;
    
    public DataNodesTreeModelGenerator(PermissionTreeView treeView,
    		boolean canModify, boolean usingRoot) {
        super();
        this.treeView = treeView;
        this.canModify = canModify;
        this.usingRoot = usingRoot;
    }
    
    public DataNodesTreeModelGenerator(DataNodesTreeModel model) {
        super();
        this.model = model;
    }
    
    public DataNodesTreeModel generateModel() throws Exception {
        DataNodesTreeModel mdl;
        if (treeView != null) {
       	    mdl = DataNodesTreeModel.createDefaultTreeModelFromTreeView(treeView, 
       	    		canModify, usingRoot);
       	} else {
       	    mdl = model;
        } 
        return mdl;
    }
}
