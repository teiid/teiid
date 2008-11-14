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

import com.metamatrix.platform.admin.api.PermissionNode;

public class DataNodePermissions {
    public final static String NONE = "(none)";

    public static DataNodePermissions getDefaultRootNodePermissions() {
        return new DataNodePermissions(false, true, false, false);
    }

    public static String[] getDefaultRootNodePermissionsStringArray() {
        return new String[] {"Read"};
    }

    private boolean create;
    private boolean read;
    private boolean update;
    private boolean delete;

    public DataNodePermissions(boolean c, boolean r, boolean u, boolean d) {
        super();
        create = c;
        read = r;
        update = u;
        delete = d;
    }

    /**
     * Constructor to create a DataNodePermissions from the array of Strings
     * contained in a PermissionNode object.
     */
    public DataNodePermissions(String[] perm) {
        create = false;
        read = false;
        update = false;
        delete = false;
        for (int i = 0; i < perm.length; i++) {
            if (perm[i].equalsIgnoreCase("Create")) {
                create = true;
            } else if (perm[i].equalsIgnoreCase("Read")) {
                read = true;
            } else if (perm[i].equalsIgnoreCase("Update")) {
                update = true;
            } else if (perm[i].equalsIgnoreCase("Delete")) {
                delete = true;
            }
        }
    }

    public DataNodePermissions(PermissionNode node) {
        this(node.getActions().getLabels());
    }

    public boolean hasCreate() {
        return create;
    }

    public boolean hasRead() {
        return read;
    }

    public boolean hasUpdate() {
        return update;
    }

    public boolean hasDelete() {
        return delete;
    }

    public Object clone() {
        return new DataNodePermissions(hasCreate(), hasRead(), hasUpdate(), hasDelete());
    }

    public boolean equals(Object obj) {
        boolean same = false;
        if (obj == this) {
            same = true;
        } else if (obj instanceof DataNodePermissions) {
            DataNodePermissions perm = (DataNodePermissions)obj;
            same = ((this.hasCreate() == perm.hasCreate()) &&
                    (this.hasRead() == perm.hasRead()) &&
                    (this.hasUpdate() == perm.hasUpdate()) &&
                    (this.hasDelete() == perm.hasDelete()));
        }
        return same;
    }

    public String toString() {
        String str = "DataNodePermissions: " + toPermissionsString();
        return str;
    }

    public String toPermissionsString() {
        String str = "";
        if (hasCreate()) {
            str += "C";
        }
        if (hasRead()) {
            str += "R";
        }
        if (hasUpdate()) {
            str += "U";
        }
        if (hasDelete()) {
            str += "D";
        }
        if (str.length() == 0) {
            str = NONE;
        }
        return str;
    }
}

