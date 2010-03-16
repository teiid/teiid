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

package com.metamatrix.query.sql.lang;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;
import com.metamatrix.query.xquery.XQueryExpression;

/**
 * An XQuery command object
 */
public class XQuery extends Command {
    
    private String xQuery;
    private XQueryExpression compiledXQuery;
    private Map variables;
    private String procedureGroup;
    
    public XQuery(){
    }

    public XQuery(String xQuery, XQueryExpression compiledXQuery){
        this.xQuery = xQuery;
        this.compiledXQuery = compiledXQuery;
    }
    
    public void setProcedureGroup(String procedureGroup) {
		this.procedureGroup = procedureGroup;
	}
    
    public void setVariables(Map variables) {
		this.variables = variables;
	}
    
    public Map getVariables() {
		return variables;
	}
    
    public String getXQuery(){
        return xQuery;
    }
    
    public XQueryExpression getCompiledXQuery(){
        return this.compiledXQuery;        
    }

    /**
     * @see com.metamatrix.query.sql.lang.Command#getType()
     */
    public int getType() {
        return Command.TYPE_XQUERY;
    }

    /**
     * @see com.metamatrix.query.sql.lang.Command#getProjectedSymbols()
     */
    public List getProjectedSymbols() {
        List selectList = new ArrayList(1);
        ElementSymbol xmlElement = new ElementSymbol("xml"); //$NON-NLS-1$
        xmlElement.setType(DataTypeManager.DefaultDataClasses.XML);
        selectList.add(xmlElement);
        return selectList;
    }

    /**
     * Compare two queries for equality.  Queries will only evaluate to equal if
     * they are IDENTICAL.  
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Quick same object test
        if(this == obj) {
            return true;
        }

        // Quick fail tests     
        if(!(obj instanceof XQuery)) {
            return false;
        }

        return EquivalenceUtil.areEqual(getXQuery(), ((XQuery)obj).getXQuery());
    }

    /**
     * Get hashcode for query.  
     * @return Hash code
     */
    public int hashCode() {
        return xQuery.hashCode();
    }

    /**
     * Return a copy of this SQLQuery.
     * @return Deep or safe clone
     */
    public Object clone() {
        XQuery copy = new XQuery(getXQuery(), this.compiledXQuery);
        copy.variables = variables;
        copy.procedureGroup = procedureGroup;
        copyMetadataState(copy);
        return copy;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }
	
	/**
	 * @see com.metamatrix.query.sql.lang.Command#areResultsCachable()
	 */
	public boolean areResultsCachable() {
		return true;
	}
    
	public String getProcedureGroup() {
		return procedureGroup;
	}
}
