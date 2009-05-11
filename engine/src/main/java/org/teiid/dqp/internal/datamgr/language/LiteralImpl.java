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

import org.teiid.connector.language.ILiteral;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;

public class LiteralImpl extends BaseLanguageObject implements ILiteral {
    
    private Object value;
    private Class<?> type;
    private boolean bindValue;
    private boolean multiValued;
    
    public LiteralImpl(Object value, Class<?> type) {
        this.value = value;
        this.type = type;
    }
    
    /**
     * @see org.teiid.connector.language.ILiteral#getValue()
     */
    public Object getValue() {
        return this.value;
    }

    /**
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.ILiteral#getType()
     */
    public Class<?> getType() {
        return this.type;
    }

    /* 
     * @see com.metamatrix.data.language.ILiteral#setValue(java.lang.Object)
     */
    public void setValue(Object value) {
        this.value = value;
    }

    /** 
     * @see org.teiid.connector.language.ILiteral#isBindValue()
     */
    public boolean isBindValue() {
        return bindValue;
    }

    /** 
     * @see org.teiid.connector.language.ILiteral#setBindValue(boolean)
     */
    public void setBindValue(boolean bindValue) {
        this.bindValue = bindValue;
    }

	@Override
	public boolean isMultiValued() {
		return multiValued;
	}

	@Override
	public void setMultiValued(boolean multiValued) {
		this.multiValued = multiValued;
	}

	@Override
	public void setType(Class<?> type) {
		this.type = type;
	}

}
