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

package com.metamatrix.platform.config.event;

import java.util.Collection;
import java.util.EventObject;
import java.util.Iterator;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConnectorBindingID;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.namedobject.BaseID;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

public class ConfigurationChangeEvent extends EventObject{
	public static final int CONFIG_REFRESH = 0;
	public static final int CONFIG_CHANGE = 1;

    private Collection ids=null;
    private BaseID id=null;
    private int action;


    public ConfigurationChangeEvent(Object source, Collection baseIDs, int action) {
        super(source);
        Assertion.isNotNull(baseIDs, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0007));

        this.action = action;
        this.ids = baseIDs;
        this.id = null;
    }

    public ConfigurationChangeEvent(Object source, int action) {
        super(source);

        this.action = action;
        this.id = null;
        this.ids = null;
    }


    public ConfigurationChangeEvent(Object source, BaseID baseID, int action) {
        super(source);
        if(baseID == null){
            Assertion.isNotNull(baseID, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0008));
        }

        this.action = action;
        this.id = baseID;
        this.ids = null;
    }


    public boolean refreshConfig() {
        return action == CONFIG_REFRESH;
    }

    public boolean configChange() {
        return action == CONFIG_CHANGE ;
    }


    public Collection getChangedIDs() {
        return ids;
    }

    public boolean isConnectorBindingChange() {
    	if (ids == null && id == null) {
    		return false;
    	}
    	if (id != null) {
    		return (id instanceof ConnectorBindingID);
    	}
    	for (Iterator it=ids.iterator(); it.hasNext(); ) {
    		BaseID bid = (BaseID) it.next();
    		if (bid instanceof ConnectorBindingID) {
    			return true;
    		}
    	}
    	return false;

    }

    public boolean isComponentTypeChange() {
    	if (ids == null && id == null) {
    		return false;
    	}
    	if (id != null) {
    		return (id instanceof ConnectorBindingID);
    	}

    	for (Iterator it=ids.iterator(); it.hasNext(); ) {
    		BaseID bid = (BaseID) it.next();
    		if (bid instanceof ComponentTypeID) {
    			return true;
    		}
    	}
    	return false;

    }

    public boolean isHostChange() {
    	if (ids == null && id == null) {
    		return false;
    	}
    	if (id != null) {
    		return (id instanceof ConnectorBindingID);
    	}

    	for (Iterator it=ids.iterator(); it.hasNext(); ) {
    		BaseID bid = (BaseID) it.next();
    		if (bid instanceof HostID) {
    			return true;
    		}
    	}
    	return false;

    }

    public boolean isServiceChange() {
    	if (ids == null && id == null) {
    		return false;
    	}
    	if (id != null) {
    		return (id instanceof ConnectorBindingID);
    	}

    	for (Iterator it=ids.iterator(); it.hasNext(); ) {
    		BaseID bid = (BaseID) it.next();
    		if (bid instanceof ServiceComponentDefnID) {
    			return true;
    		}
    	}
    	return false;
    }


}
