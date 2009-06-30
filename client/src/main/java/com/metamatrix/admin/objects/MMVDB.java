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

package com.metamatrix.admin.objects;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import org.teiid.adminapi.VDB;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.core.util.HashCodeUtil;

/**
 * Dataholder object for information about a VDB (Virtual Database)
 */
public final class MMVDB extends MMAdminObject implements VDB, Comparable {

    /**VDB is registered but incomplete*/
    public static final int STATUS_INCOMPLETE = 1;
    /**VDB is deployed but inactive*/
    public static final int STATUS_INACTIVE   = 2;
    /**VDB is deployed and active*/
    public static final int STATUS_ACTIVE     = 3;
    /**VDB has been deleted*/
    public static final int STATUS_DELETED    = 4;

    final static String[] VDB_STATUS_NAMES = {"Incomplete",  //$NON-NLS-1$
                                              "Inactive",   //$NON-NLS-1$
                                              "Active", //$NON-NLS-1$
                                              "Deleted"}; //$NON-NLS-1$

    
    
    private Collection models = new ArrayList();
    private short status;
    private Date versionedDate;
    private String versionedBy;
    private long uid;
    private boolean hasMaterializedViews;
    private int cachedHashcode;
    private boolean hasWSDL;
    
    
  
    /**
     * VDBs are identified by name and version.
     * @param identifierParts the VDB name and version parts
     */
    public MMVDB(String[] identifierParts) {
        super(identifierParts);
        this.cachedHashcode = HashCodeUtil.hashCode(13, getIdentifier());
    }
    
    /** 
     * @see java.lang.Object#toString()
     * @since 4.3
     */
    public String toString() {
        StringBuffer result = new StringBuffer();
        result.append(AdminPlugin.Util.getString("MMVDB.MMVDB")).append(getIdentifier()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMVDB.status")).append(getState()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMVDB.versionedDate")).append(versionedDate); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMVDB.versionedBy")).append(versionedBy); //$NON-NLS-1$
      	result.append(AdminPlugin.Util.getString("MMVDB.properties")).append(getPropertiesAsString()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMVDB.models")).append(models); //$NON-NLS-1$
        return result.toString();
    }

    /**
     * Add an MMModel 
     * @param mmModel
     * @since 4.3
     */
    public void addModel(MMModel mmModel) {
        models.add(mmModel);
    }

	/**
	 * @return date the VDB was versioned
	 */
	public Date getVersionedDate() {
		return versionedDate;
	}

	/**
	 * @return user that versioned the VDB
	 */
	public String getVersionedBy() {
		return versionedBy;
	}

	/**
	 * @return Collection of MMModels
	 */
	public Collection getModels() {
		return models;
	}

    /**
     * @return the status
     */
    public int getState() {
        return status;
    }

    /**
     * @return the status
     */
    public String getStateAsString() {
        return VDB_STATUS_NAMES[this.status - 1];
    }

    /** 
     * Must be overridden since, unlike other admin objects, the
     * name component of a VDB is the first component of the
     * identifier.  VDB version is the second component.
     * @see com.metamatrix.admin.objects.MMAdminObject#getName()
     * @return The Name of the VDB
     * @since 4.3
     */
    public String getName() {
        return identifierParts[0];
    }
    
	/**
	 * @return the VDB version
	 */
	public String getVDBVersion() {
		return identifierParts[1];
	}

    
    /** 
     * @param bound The date the VDB was bound.
     * @since 4.3
     */
    public void setVersionedDate(Date bound) {
        this.versionedDate = bound;
    }

    
    /** 
     * @param boundBy The user that bound the VDB.
     * @since 4.3
     */
    public void setVersionedBy(String boundBy) {
        this.versionedBy = boundBy;
    }

    
    /** 
     * @param models Collection of MMModels to set.
     * @since 4.3
     */
    public void setModels(Collection models) {
        this.models = models;
    }

    
    /** 
     * @param status The status to set.
     * @since 4.3
     */
    public void setStatus(short status) {
        this.status = status;
        
        //TODO: are these correct?
        setEnabled(status == STATUS_ACTIVE);
        setRegistered(status == STATUS_ACTIVE || status == STATUS_INACTIVE || status == STATUS_INCOMPLETE);
    }

    
    /** 
     * @return Returns the uid.
     * @since 4.3
     */
    public long getUID() {
        return this.uid;
    }

    
    /** 
     * @param uid The uid to set.
     * @since 4.3
     */
    public void setUID(long uid) {
        this.uid = uid;
    }

    /** 
     * @see org.teiid.adminapi.VDB#hasMaterializedViews()
     * @since 4.3
     */
    public boolean hasMaterializedViews() {
        return hasMaterializedViews;
    }

    
    /** 
     * @param hasMaterializedViews The hasMaterializedViews to set.
     * @since 4.3
     */
    public void setMaterializedViews(boolean hasMaterializedViews) {
        this.hasMaterializedViews = hasMaterializedViews;
    }

    
    
    
    /** 
     * @return Returns the hasWSDL.
     * @since 5.5.3
     */
    public boolean hasWSDL() {
        return this.hasWSDL;
    }

    
    /** 
     * @param hasWSDL The hasWSDL to set.
     * @since 5.5.3
     */
    public void setHasWSDL(boolean hasWSDL) {
        this.hasWSDL = hasWSDL;
    }

    /** 
     * @see java.lang.Object#equals(java.lang.Object)
     * @since 4.3
     */
    public boolean equals(Object obj) {
        if ( ! (obj instanceof MMVDB) ) {
            return false;
        }
        
        MMVDB other = (MMVDB)obj;
        if ( ! identifier.equals(other.identifier) ) {
            return false;
        }
        
        return true;
    }

    /** 
     * @see java.lang.Object#hashCode()
     * @since 4.3
     */
    public int hashCode() {
        return this.cachedHashcode;
    }

    /** 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     * @since 4.3
     */
    public int compareTo(Object obj) {
        if ( ! (obj instanceof MMVDB) ) {
            return -1;
        }
        
       MMVDB other = (MMVDB)obj;
        return identifier.compareTo(other.identifier);
    }

    
    
}
