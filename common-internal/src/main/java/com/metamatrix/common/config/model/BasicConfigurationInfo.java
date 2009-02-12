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

package com.metamatrix.common.config.model;

import java.io.Serializable;
import java.util.Date;

import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationInfo;
import com.metamatrix.common.namedobject.BasicObject;


public class BasicConfigurationInfo extends BasicObject implements ConfigurationInfo, Serializable {
    private boolean isDeployed;
    private boolean isReleased;
    private boolean isLocked;
    private Date creationDate = new Date();
    private Date lastChangedDate;


    public BasicConfigurationInfo(ConfigurationID id) {
	    super(id);
    }

    BasicConfigurationInfo(BasicConfigurationInfo info) {
        super(info.getID());
        this.isDeployed = info.isDeployed();
        this.isReleased = info.isReleased();
        this.isLocked = info.isLocked();
        this.creationDate = info.getCreationDate();
        this.lastChangedDate = info.getLastChangedDate();
    }

    public Date getLastChangedDate() {
	    return lastChangedDate;
    }
    public Date getCreationDate() {
	    return creationDate;
    }
    public boolean isDeployed() {
	    return isDeployed;
    }
    public boolean isLocked() {
	    return isLocked;
    }
    public boolean isReleased() {
	    return isReleased;
    }
    

    void setName(String name) {
        ConfigurationID id = new ConfigurationID(name);
        this.setID(id);
    }

    void setIsDeployed(boolean deploy) {
	    isDeployed = deploy;
    }
    void setIsLocked(boolean locked) {
	    isLocked = locked;
    }
    void setIsReleased(boolean release) {
	    isReleased = release;
    }

    void setCreationDate(Date created){
        creationDate = created;
    }

    void setLastChangedDate(Date lastChanged){
        lastChangedDate = lastChanged;
    }

    public synchronized Object clone() {

       	return new BasicConfigurationInfo(this);
    }

}

