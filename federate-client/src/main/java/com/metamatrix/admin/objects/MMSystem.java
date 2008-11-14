/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.admin.objects;

import java.util.Date;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.admin.api.objects.SystemObject;
import com.metamatrix.core.util.DateUtil;

/**
 * Dataholder for information about the system at-large
 */
public class MMSystem extends MMAdminObject implements SystemObject {

    
    Date startTime;
    boolean isStarted;
    
    
    /**Dummy identifier for the system as a whole.*/
    private final static String SYSTEM_IDENTIFIER = "SYSTEM"; //$NON-NLS-1$
    
    /**
     * Construct a new MMSystem object 
     * 
     * @since 4.3
     */
    public MMSystem() {
        super(new String[] {SYSTEM_IDENTIFIER});
    }
    
 
    
    
    /** 
     * @return Returns whether the system is started.
     * @since 4.3
     */
    public boolean isStarted() {
        return this.isStarted;
    }
    
    /** 
     * @param isStarted whether the system is started.
     * @since 4.3
     */
    public void setStarted(boolean isStarted) {
        this.isStarted = isStarted;
    }

       

    
    /** 
     * @return Returns the startTime.
     * @since 4.3
     */
    public Date getStartTime() {
        return this.startTime;
    }

    
    /** 
     * @param startTime The startTime to set.
     * @since 4.3
     */
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    /**
     * Get The Start Date as a String 
     * @return String 
     * @since 4.3
     */
    public String getStartDateAsString() {
        if( this.startTime != null)
            return DateUtil.getDateAsString(getStartTime());
        return "Start Date not Set"; //$NON-NLS-1$
    }


    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuffer result = new StringBuffer();
        result.append(AdminPlugin.Util.getString("MMSystem.MMSystem")).append(getIdentifier());  //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMSystem.properties")).append(getPropertiesAsString()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMSystem.isStarted")).append(isStarted()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMSystem.startTime")).append(getStartDateAsString()); //$NON-NLS-1$
        return result.toString();
    }


}

