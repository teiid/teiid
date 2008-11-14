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

import javax.swing.tree.TreePath;

import com.metamatrix.platform.admin.api.PermissionNode;

public class DataNodeInfo {
    private TreePath treePathToNode;
    private boolean isRoot;
    private DataNodePermissions permissions;
    private DataNodePermissions permissionsInDB;
    private PermissionNode serverSidePermissions;
    private String vdbName;
    private int vdbVersion;


    public DataNodeInfo(TreePath tp, boolean root, DataNodePermissions perm,
            String vdbNam, int vdbVers, PermissionNode servPerm) {
        treePathToNode = tp;
        isRoot = root;
        permissions = perm;
        vdbName = vdbNam;
        vdbVersion = vdbVers;
        serverSidePermissions = servPerm;
        setInDBFlags();
    }

    private void setInDBFlags() {
        //If permissions object null, is root node
        if (serverSidePermissions == null) {
            permissionsInDB = null;
        } else {
            permissionsInDB = new DataNodePermissions(
                    serverSidePermissions.getActions().getLabels());
        }
    }

    public TreePath getTreePathToNode() {
        return treePathToNode;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public void setPermissions(DataNodePermissions perm) {
        permissions = perm;
    }

    public DataNodePermissions getPermissions() {
        return permissions;
    }

    public void setPermissionsInDB(DataNodePermissions perm) {
        permissionsInDB = perm;
    }

    public DataNodePermissions getPermissionsInDB() {
        return permissionsInDB;
    }

    public String getVDBName() {
        return vdbName;
    }

    public int getVDBVersions() {
        return vdbVersion;
    }

    public PermissionNode getServerSidePermissions() {
        return serverSidePermissions;
    }
}
