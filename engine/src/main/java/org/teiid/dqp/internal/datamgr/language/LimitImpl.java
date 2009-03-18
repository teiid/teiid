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

package org.teiid.dqp.internal.datamgr.language;

import org.teiid.connector.language.ILimit;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;


/** 
 * @since 4.3
 */
public class LimitImpl extends BaseLanguageObject implements ILimit {

    private int rowOffset;
    private int rowLimit;
    
    public LimitImpl(int offset, int rowLimit) {
        this.rowOffset = offset;
        this.rowLimit = rowLimit;
    }
    /** 
     * @see org.teiid.connector.language.ILimit#getRowLimit()
     * @since 4.3
     */
    public int getRowLimit() {
        return rowLimit;
    }

    /** 
     * @see org.teiid.connector.language.ILimit#getRowOffset()
     * @since 4.3
     */
    public int getRowOffset() {
        return rowOffset;
    }

    /** 
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(org.teiid.connector.visitor.framework.LanguageObjectVisitor)
     * @since 4.3
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

}
