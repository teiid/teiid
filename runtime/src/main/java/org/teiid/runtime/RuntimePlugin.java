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
package org.teiid.runtime;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;


public class RuntimePlugin {
    private static final String PLUGIN_ID = "org.teiid.runtime" ; //$NON-NLS-1$
    private static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$
    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,BUNDLE_NAME,ResourceBundle.getBundle(BUNDLE_NAME));

    public static enum Event implements BundleUtil.Event{
    	TEIID40001, // undefined translator properties
    	TEIID40002, // failed to load ODBC metadata
    	TEIID40003, // VDB status
    	TEIID40007, // keep alive failed
    	TEIID40008, // expired session
    	TEIID40009, // terminate session
    	TEIID40011, // processing error
    	TEIID40012, // data source not found
    	TEIID40013, // replication failed
    	TEIID40014, // krb5 failed
    	TEIID40015, // pg error
    	TEIID40016, // pg ssl error
    	TEIID40017, // unexpected exp for session
    	TEIID40018,
    	TEIID40020,
    	TEIID40021,
    	TEIID40022,
    	TEIID40024,
    	TEIID40025,
    	TEIID40026,
    	TEIID40027,
    	TEIID40028,
    	TEIID40029,
    	TEIID40031,
    	TEIID40032,
    	TEIID40033,
    	TEIID40034,
    	TEIID40035,
    	TEIID40039,
    	TEIID40041,
    	TEIID40042,
    	TEIID40043,
    	TEIID40044,
    	TEIID40045,
    	TEIID40046,
    	TEIID40047,
    	TEIID40048,
    	TEIID40051,
    	TEIID40052,
    	TEIID40053,
    	TEIID40054,
    	TEIID40055,
    	TEIID40059,
    	TEIID40062,
    	TEIID40063,
    	TEIID40064,
    	TEIID40065,
    	TEIID40067,
    	TEIID40069,
    	TEIID40070,
    	TEIID40071,    	
    	TEIID40072,
    	TEIID40073,
    	TEIID40074,
    	TEIID40075,
    	TEIID40076,
    	TEIID40077,
    	TEIID40078,
    	TEIID40079,
    	TEIID40080,
    	TEIID40081,
    	TEIID40082,
    	TEIID40083, //vdb import does not exist
    	TEIID40084, //imported role conflict
    	TEIID40085, //imported model conflict
    	TEIID40086, //imported connector manager conflict
    	TEIID40087, //pass-through failed
    	TEIID40088, //event distributor replication failed
    	TEIID40089, //txn disabled
    	TEIID40090,
    	TEIID40091,
    	TEIID40092,
    	TEIID40093, //no sources
    	TEIID40094, //invalid metadata repso
    	TEIID40095, //deployment failed
    	TEIID40096, //vdb deploy timeout
    	TEIID40097, //vdb finish timeout  
    	TEIID40098,
    	TEIID40099,
    	TEIID40100,
    	TEIID40101,
    	TEIID40102,
    	TEIID40103,
    	TEIID40104, 
    	TEIID40105, 
    	TEIID40106, //override translators not allowed in embedded
    	TEIID40107, 
    	TEIID40108, 
    	TEIID40109,
    }
}
