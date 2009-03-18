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

import java.util.Collections;
import java.util.List;

import org.teiid.connector.language.IBulkInsert;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;



/**
 * A simple variation of insert where multiple rows can be inserted as single
 * operation.
 */
public class BulkInsertImpl extends InsertImpl implements IBulkInsert {
    List rowValues = null;
        
    public BulkInsertImpl(IGroup group, List elements) {
        super(group, elements, null);
    }
    
    public BulkInsertImpl(IGroup group, List elements, List rows) {
        super(group, elements, null);
        this.rowValues = rows;
    }
        
    /**
     * @see org.teiid.connector.language.IBulkInsert#getRows()
     */
    public List getRows() {
        if (rowValues == null) {
            return Collections.EMPTY_LIST;
        }
        return rowValues;
    }
    
    /**
     * Set the list of row values for this bulk insert  
     * @return list; never null
     */
    public void setRows(List rows) {
        rowValues = rows;
    }
    
    /**
     * @see org.teiid.connector.language.IInsert#getValues()
     */
    public List getValues() {
        throw new UnsupportedOperationException("This operation is not supported"); //$NON-NLS-1$
    }
    /**
     * @see org.teiid.connector.language.IInsert#setValues(java.util.List)
     */
    public void setValues(List values) {
        throw new UnsupportedOperationException("This operation is not supported"); //$NON-NLS-1$        
    }
    
    /**
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }       
}
