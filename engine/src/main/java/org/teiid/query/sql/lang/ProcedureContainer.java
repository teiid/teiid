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

import java.util.HashSet;
import java.util.Set;

import org.teiid.query.validator.UpdateValidator.UpdateInfo;


public abstract class ProcedureContainer extends Command implements TargetedCommand {

    private int updateCount = -1;
    private UpdateInfo updateInfo;
    private Set<String> tags;
    
    protected void copyMetadataState(ProcedureContainer copy) {
        super.copyMetadataState(copy);
        copy.setUpdateInfo(this.getUpdateInfo());
        copy.updateCount = updateCount;
        if (tags != null) {
            copy.tags = new HashSet<String>(tags);
        }
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
    
    public UpdateInfo getUpdateInfo() {
		return updateInfo;
	}
    
    public void setUpdateInfo(UpdateInfo updateInfo) {
		this.updateInfo = updateInfo;
	}
    
    public boolean hasTag(String name) {
        return tags != null && tags.contains(name);
    }
    
    public void addTag(String name) {
        if (tags == null) {
            tags = new HashSet<String>();
        }
        tags.add(name);
    }
    
}
