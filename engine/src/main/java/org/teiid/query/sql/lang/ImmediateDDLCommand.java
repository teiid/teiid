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

import java.util.List;

import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;

public final class ImmediateDDLCommand extends Command {

    private final String sql;

    public ImmediateDDLCommand(String sql) {
        this.sql = sql;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        
    }

    @Override
    public boolean areResultsCachable() {
        return false;
    }

    @Override
    public Object clone() {
        return this;
    }

    @Override
    public List<Expression> getProjectedSymbols() {
        return Command.getUpdateCommandSymbol();
    }

    @Override
    public int getType() {
        return Command.TYPE_IMMEDIATE_DDL;
    }

    @Override
    public String toString() {
        return sql;
    }
    
    @Override
    public int hashCode() {
        return sql.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ImmediateDDLCommand other = (ImmediateDDLCommand) obj;
        return this.sql.equals(other.sql);
    }
}