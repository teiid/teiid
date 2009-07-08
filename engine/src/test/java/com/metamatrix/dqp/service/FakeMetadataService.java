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

package com.metamatrix.dqp.service;

import java.util.HashMap;
import java.util.Map;

import org.teiid.connector.metadata.runtime.DatatypeRecordImpl;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.unittest.FakeMetadataFactory;

/**
 */
public class FakeMetadataService extends FakeAbstractService implements MetadataService {

    /**
     * Map of vdbname.vdbversion -> QueryMetadataInterface 
     */
    private Map vdbMap = new HashMap();
    
    /**
     * 
     */
    public FakeMetadataService() {
        super();
        
        // Load some default VDBs
        addVdb(null, null, FakeMetadataFactory.exampleBQT());
        addVdb("bqt", "1", FakeMetadataFactory.exampleBQT()); //$NON-NLS-1$ //$NON-NLS-2$
        addVdb("example1", "1", FakeMetadataFactory.example1()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private String getKey(String vdbName, String vdbVersion) {
        if(vdbName == null) {
            vdbName = ""; //$NON-NLS-1$
        } 
        if(vdbVersion == null) {
            vdbVersion = ""; //$NON-NLS-1$
        }
        String vdbID = vdbName + "." + vdbVersion; //$NON-NLS-1$
        return vdbID.toLowerCase();        
    }

    public synchronized void addVdb(String vdbName, String vdbVersion, QueryMetadataInterface metadata) {
        this.vdbMap.put(getKey(vdbName, vdbVersion), metadata);
    }

    /* 
     * @see com.metamatrix.dqp.service.MetadataService#lookupMetadata(java.lang.String, java.lang.String)
     */
    public synchronized QueryMetadataInterface lookupMetadata(String vdbName, String vdbVersion) {
        return (QueryMetadataInterface) vdbMap.get(getKey(vdbName, vdbVersion));
    }

	@Override
	public IObjectSource getMetadataObjectSource(String vdbName,
			String vdbVersion) throws MetaMatrixComponentException {
		return null;
	}
	
	@Override
	public Map<String, DatatypeRecordImpl> getBuiltinDatatypes()
			throws MetaMatrixComponentException {
		return null;
	}

}
