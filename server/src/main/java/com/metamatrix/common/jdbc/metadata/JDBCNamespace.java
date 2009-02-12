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

package com.metamatrix.common.jdbc.metadata;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.metamatrix.core.util.ArgCheck;

abstract public class JDBCNamespace extends JDBCObject {
    private List contents;

    protected JDBCNamespace() {
        super();
        this.contents = new LinkedList();
    }

    protected JDBCNamespace(String name) {
        super(name);
        this.contents = new LinkedList();
    }

    public List getContents() {
        return this.contents;
    }

    protected void addContent(JDBCObject object) {
        if (!this.contents.contains(object)) {
            this.contents.add(object);
            object.setOwner(this);
        }
    }

    protected boolean removeContent(JDBCObject object) {
        ArgCheck.isTrue(object.getOwner() == this, "The specified object is not contained by this object"); //$NON-NLS-1$
        object.setOwner(null);
        return this.contents.remove(object);
    }

    protected boolean hasContent(JDBCObject object) {
        return this.contents.contains(object);
    }

    protected JDBCObject lookupContent(String name, Class type) {
        return JDBCObject.lookupJDBCObject(this.contents,name,type);
    }

    public void print(PrintStream stream) {
        print(stream, "  "); //$NON-NLS-1$
    }

    public void print(PrintStream stream, String lead) {
        super.print(stream,lead);
        JDBCObject child = null;
        Iterator iter = this.contents.iterator();
        while (iter.hasNext()) {
            child = (JDBCObject)iter.next();
            child.print(stream, lead + "  "); //$NON-NLS-1$
        }
    }

}




