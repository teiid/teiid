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

/*
 * Created on Mar 23, 2004
 */
package com.metamatrix.connector.jdbc.oracle.spatial;

import java.util.List;

import com.metamatrix.data.language.ISelect;
import com.metamatrix.data.visitor.framework.LanguageObjectVisitor;

public class SpatialSelectProxy implements
                               ISelect,
                               SpatialHint {

    private ISelect delegate;

    private String hint;

    public SpatialSelectProxy(ISelect delegate,
                              String hint) {
        this.delegate = delegate;
        this.hint = hint;
    }

    public List getSelectSymbols() {
        return this.delegate.getSelectSymbols();
    }

    public boolean isDistinct() {
        return this.delegate.isDistinct();
    }

    public void setSelectSymbols(List symbols) {
        this.delegate.setSelectSymbols(symbols);
    }

    public void setDistinct(boolean distinct) {
        this.setDistinct(distinct);
    }

    public String getHint() {
        return this.hint;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.metamatrix.data.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.framework.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

}