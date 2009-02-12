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

package com.metamatrix.internal.core.xml.vdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Class that represents the content of an MetaMatrix-VdbManifestModel.xmi header. 
 */
public final class VdbHeader {
    
    public static final String SEVERITY_OK      = "OK"; //$NON-NLS-1$
    public static final String SEVERITY_INFO    = "INFO"; //$NON-NLS-1$
    public static final String SEVERITY_WARNING = "WARNING"; //$NON-NLS-1$
    public static final String SEVERITY_ERROR   = "ERROR"; //$NON-NLS-1$
    public static final Map severityNameToValueMap = new HashMap();
    static {
        severityNameToValueMap.put(SEVERITY_OK,      new Integer(0));
        severityNameToValueMap.put(SEVERITY_INFO,    new Integer(1));
        severityNameToValueMap.put(SEVERITY_WARNING, new Integer(2));
        severityNameToValueMap.put(SEVERITY_ERROR,   new Integer(3));
    }
    private static final String DEFAULT_SEVERITY = SEVERITY_OK;

    private String name;
    private String uuid;
    private String description;
    private String severity;
    private String timeLastChanged;
    private String timeLastProduced;
    private String producerName;
    private String producerVersion;
    private List<VdbModelInfo> modelInfos = new ArrayList<VdbModelInfo>();
    private List<VdbNonModelInfo> nonModelInfos = new ArrayList<VdbNonModelInfo>();
    private String xmiVersion;
    
    /**
     * Constructor for VdbHeader.
     */
    public VdbHeader() {
        this.severity      = DEFAULT_SEVERITY;
    }

    public VdbModelInfo[] getModelInfos() {
    	return modelInfos.toArray(new VdbModelInfo[modelInfos.size()]);
    }

    public VdbNonModelInfo[] getNonModelInfos() {
    	return nonModelInfos.toArray(new VdbNonModelInfo[nonModelInfos.size()]);
    }
    
    public String getName() {
        return this.name;
    }

    /**
     * The UUID associated with the VDB is the VirtualDatabase UUID
     * and not the ModelAnnotation UUID in the MetaMatrix-VdbManifestModel.xmi
     * header. 
     */
    public String getUUID() {
        return this.uuid;
    }
    
    public String getDescription() {
        return this.description;
    }
    
    public String getSeverity() {
        return this.severity;
    }
    
    public int getSeverityCode() {
        return this.getSeverityValue().intValue();
    }
    
    public String getXmiVersion() {
        return this.xmiVersion;
    }
    
    public String getProducerName() {
        return this.producerName;
    }

    public String getProducerVersion() {
        return this.producerVersion;
    }

    public String getTimeLastChanged() {
        return this.timeLastChanged;
    }

    public String getTimeLastProduced() {
        return this.timeLastProduced;
    }
    
    public void addModelInfo(final VdbModelInfo modelInfo) {
        if ( modelInfo != null && !this.modelInfos.contains(modelInfo) ) {
            this.modelInfos.add( modelInfo );
        }
    }
    
    public void addNonModelInfo(final VdbNonModelInfo nonModelInfo) {
        if ( nonModelInfo != null && !this.nonModelInfos.contains(nonModelInfo) ) {
            this.nonModelInfos.add( nonModelInfo );
        }
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public void setUUID(final String uuid) {
        this.uuid = uuid;
    }
    
    public void setDescription(final String description) {
        this.description = description;
    }
    
    public void setSeverity(final String severity) {
        this.severity = severity;
    }
    
    public void setXmiVersion(final String xmiVersion) {
        this.xmiVersion = xmiVersion;
    }
        
    public void setProducerName(String producerName) {
        this.producerName = producerName;
    }
    
    public void setProducerVersion(String producerVersion) {
        this.producerVersion = producerVersion;
    }
    
    public void setTimeLastChanged(String timeLastChanged) {
        this.timeLastChanged = timeLastChanged;
    }
    
    public void setTimeLastProduced(String timeLastProduced) {
        this.timeLastProduced = timeLastProduced;
    }
    
    protected Integer getSeverityValue() {
        Integer value = (Integer)severityNameToValueMap.get(this.getSeverity());
        return (value != null ? value : (Integer)severityNameToValueMap.get(DEFAULT_SEVERITY));
    }

    /**
     * Method to print the contents of the MetaMatrix-VdbManifestModel Header object.
     * @param stream the stream
     */
    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append("MetaMatrix-VdbManifestModel Header:"); //$NON-NLS-1$
        sb.append("\n  XMI version: "); //$NON-NLS-1$
        sb.append(this.getXmiVersion() );
        sb.append("\n  Name:        "); //$NON-NLS-1$
        sb.append(this.getName() );
        sb.append("\n  UUID:        "); //$NON-NLS-1$
        sb.append(this.getUUID() );
        sb.append("\n  Description: "); //$NON-NLS-1$
        sb.append(this.getDescription() );
        sb.append("\n  Severity: "); //$NON-NLS-1$
        sb.append(this.getSeverity() );
        sb.append("\n  Producer Name: "); //$NON-NLS-1$
        sb.append(this.getProducerName() );
        sb.append("\n  Producer Version: "); //$NON-NLS-1$
        sb.append(this.getProducerVersion() );
        sb.append("\n  Time last changed: "); //$NON-NLS-1$
        sb.append(this.getTimeLastChanged() );
        sb.append("\n  Time last produced: "); //$NON-NLS-1$
        sb.append(this.getTimeLastProduced() );
        sb.append("\n  VdbModelInfos:"); //$NON-NLS-1$
        VdbModelInfo[] mdlInfos = this.getModelInfos();
        for (int i = 0; i < mdlInfos.length; i++) {
            sb.append("\n    "); //$NON-NLS-1$
            sb.append(mdlInfos[i]);
       }
        sb.append("\n  VdbNonModelInfos:"); //$NON-NLS-1$
        VdbNonModelInfo[] nonMdlInfos = this.getNonModelInfos();
        for (int i = 0; i < nonMdlInfos.length; i++) {
            sb.append("\n    "); //$NON-NLS-1$
            sb.append(nonMdlInfos[i]);
       }
       return sb.toString();
    }
}

