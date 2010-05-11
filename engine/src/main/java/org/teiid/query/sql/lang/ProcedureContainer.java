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

import java.util.Map;

import org.teiid.query.sql.symbol.GroupSymbol;


public abstract class ProcedureContainer extends Command {

    private int updateCount = -1;
    
    public abstract GroupSymbol getGroup();
    
    protected void copyMetadataState(ProcedureContainer copy) {
        super.copyMetadataState(copy);
        copy.updateCount = updateCount;
    }
    
    /** 
     * @return Returns the updateCount.
     */
    public int getUpdateCount() {
        return this.updateCount;
    }

    
    /** 
     * @param updateCount The updateCount to set.
     */
    public void setUpdateCount(int updateCount) {
        if (updateCount < 0) {
            return;
        }
        if (updateCount > 2) {
            updateCount = 2;
        }
        this.updateCount = updateCount;
    }
    
    public abstract Map getProcedureParameters();
}
