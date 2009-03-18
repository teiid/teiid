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

import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;

public class SelectSymbolImpl extends BaseLanguageObject implements ISelectSymbol {

    private boolean hasAlias;
    private String name;
    private IExpression expression = null;
    
    public SelectSymbolImpl(String name, IExpression expression) {
        this.name = name;
        this.expression = expression;
    }
    /**
     * @see org.teiid.connector.language.ISelectSymbol#hasAlias()
     */
    public boolean hasAlias() {
        return hasAlias;
    }

    public void setAlias(boolean alias){
        this.hasAlias = alias;    
    }
    
    /**
     * @see org.teiid.connector.language.ISelectSymbol#getOutputName()
     */
    public String getOutputName() {
        return name;
    }

    /**
     * @see org.teiid.connector.language.ISelectSymbol#getExpression()
     */
    public IExpression getExpression() {
        return expression;
    }

    /**
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.ISelectSymbol#setOutputName(java.lang.String)
     */
    public void setOutputName(String name) {
        this.name = name;
    }
    
    /* 
     * @see com.metamatrix.data.language.ISelectSymbol#setExpression(com.metamatrix.data.language.IExpression)
     */
    public void setExpression(IExpression expression) {
        this.expression = expression;
    }

}
