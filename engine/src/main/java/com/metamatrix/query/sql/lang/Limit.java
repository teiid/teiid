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

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;


public class Limit implements LanguageObject {
    
    private Expression offset;
    private Expression rowLimit;
    
    public Limit(Expression offset, Expression rowLimit) {
        this.offset = offset;
        this.rowLimit = rowLimit;
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
        if (this.offset == null) {
            if (other.offset != null
                && !(other.offset instanceof Constant && ((Constant)other.offset).getValue().equals(new Integer(0)))) {
                return false;
            }
        } else if (this.offset instanceof Constant) {
            if (other.offset == null) {
                if (!((Constant)this.offset).getValue().equals(new Integer(0))) {
                    return false;
                }
            } else if (!this.offset.equals(other.offset)) {
                return false;
            }
        } else if (!EquivalenceUtil.areEqual(this.offset, other.offset)) {
            return false;
        }
        return EquivalenceUtil.areEqual(this.rowLimit, other.rowLimit);
    }
    
    public Object clone() {
        if (offset == null) {
            return new Limit(null, (Expression)rowLimit.clone());
        }
        return new Limit((Expression)offset.clone(), (Expression)rowLimit.clone());
    }
    
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }
}
