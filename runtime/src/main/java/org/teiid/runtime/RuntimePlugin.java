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
    	TEIID40003, // VDB Active
    	TEIID40004, // VDB validity errors
    	TEIID40005, // datasource or translator not found
    	TEIID40006, // VDB inactive
    	TEIID40007, // keep alive failed
    	TEIID40008, // expired session
    	TEIID40009, // terminate session
    	TEIID40010, // odbc error
    	TEIID40011, // processing error
    	TEIID40012, // data source not found
    	TEIID40013, // replication failed
    	TEIID40014, // krb5 failed
    	TEIID40015, // pg error
    	TEIID40016, // pg ssl error
    	TEIID40017, // unexpected exp for session
    	TEIID40018,
    	TEIID40019,
    	TEIID40020,
    	TEIID40021,
    	TEIID40022,
    	TEIID40023,
    	TEIID40024,
    	TEIID40025,
    	TEIID40026,
    	TEIID40027,
    	TEIID40028,
    	TEIID40029,
    	TEIID40030,
    	TEIID40031,
    	TEIID40032,
    	TEIID40033,
    	TEIID40034,
    	TEIID40035,
    	TEIID40036,
    	TEIID40037,
    	TEIID40038,
    	TEIID40039,
    	TEIID40040,
    	TEIID40041,
    	TEIID40042,
    	TEIID40043,
    	TEIID40044,
    	TEIID40045,
    	TEIID40046,
    	TEIID40047,
    	TEIID40048,
    	TEIID40049,
    	TEIID40050,
    	TEIID40051,
    	TEIID40052,
    	TEIID40053,
    	TEIID40054,
    	TEIID40055,
    	TEIID40056,
    	TEIID40057,
    	TEIID40058,
    	TEIID40059,
    	TEIID40060,
    	TEIID40061,
    	TEIID40062,
    	TEIID40063,
    	TEIID40064,
    	TEIID40065,
    	TEIID40066,
    	TEIID40067,
    	TEIID40068,
    	TEIID40069,
    	TEIID40070,
    	TEIID40071,    	
    	TEIID40072,
    	TEIID40073
    }
}
