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

package org.teiid.query.sql.lang;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.SQLStringVisitor;



public class Limit implements LanguageObject {
	
	public static String NON_STRICT = "NON_STRICT"; //$NON-NLS-1$
    
    private Expression offset;
    private Expression rowLimit;
    private boolean implicit;
    private boolean strict = true;
    
    public Limit(Expression offset, Expression rowLimit) {
        this.offset = offset;
        this.rowLimit = rowLimit;
    }
    
    private Limit() {
    	
    }
    
    public void setStrict(boolean strict) {
		this.strict = strict;
	}
    
    public boolean isStrict() {
		return strict;
	}
    
    public boolean isImplicit() {
		return implicit;
	}
    
    public void setImplicit(boolean implicit) {
		this.implicit = implicit;
	}
    
    public Expression getOffset() {
        return offset;
    }
    
    public void setOffset(Expression offset) {
        this.offset = offset;
    }
    
    public Expression getRowLimit() {
        return rowLimit;
    }
    
    public void setRowLimit(Expression rowLimit ) {
        this.rowLimit = rowLimit;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
    
    public int hashCode() {
        int h = HashCodeUtil.hashCode(0, offset);
        return HashCodeUtil.hashCode(h, rowLimit);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Limit)) {
            return false;
        }
        Limit other = (Limit)o;
        if (!EquivalenceUtil.areEqual(this.offset, other.offset)) {
            return false;
        }
        return EquivalenceUtil.areEqual(this.rowLimit, other.rowLimit);
    }
    
    public Limit clone() {
        Limit clone = new Limit();
        clone.implicit = this.implicit;
        clone.strict = this.strict;
        if (this.rowLimit != null) {
        	clone.setRowLimit((Expression) this.rowLimit.clone());
        }
        if (this.offset != null) {
        	clone.setOffset((Expression) this.offset.clone());
        }
        return clone;
    }
    
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }
}
