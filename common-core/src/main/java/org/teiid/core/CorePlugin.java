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

package org.teiid.core;

import java.util.ResourceBundle;


/**
 * CorePlugin
 */
public class CorePlugin {
    //
    // Class Constants:
    //
    /**
     * The plug-in identifier of this plugin
     */
    public static final String PLUGIN_ID = CorePlugin.class.getPackage().getName();

	public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,
	                                                     PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$
	
	public static enum Event implements BundleUtil.Event {
		TEIID10000,
		TEIID10001,
		TEIID10002,
		TEIID10003,
		TEIID10004,
		TEIID10005,
		TEIID10006,
		TEIID10007,
		TEIID10008,
		TEIID10009,
		TEIID10010,
		TEIID10011,
		TEIID10012,
		TEIID10013,
		TEIID10014,
		TEIID10015,
		TEIID10016,
		TEIID10017,
		TEIID10018,
		TEIID10021,
		TEIID10022,
		TEIID10023,
		TEIID10024,
		TEIID10030,
		TEIID10032,
		TEIID10033,
		TEIID10034,
		TEIID10035,
		TEIID10036,
		TEIID10037,
		TEIID10038,
		TEIID10039,
		TEIID10040,
		TEIID10041,
		TEIID10042,
		TEIID10043,
		TEIID10044,
		TEIID10045,
		TEIID10046,
		TEIID10047,
		TEIID10048,
		TEIID10049,
		TEIID10051,
		TEIID10052,
		TEIID10053,
		TEIID10054,
		TEIID10056,
		TEIID10057,
		TEIID10058,
		TEIID10059,
		TEIID10060,
		TEIID10061,
		TEIID10063,
		TEIID10068,
		TEIID10070,
		TEIID10071,
		TEIID10072,
		TEIID10073,
		TEIID10074,
		TEIID10076,
		TEIID10077,
		TEIID10078,
		TEIID10080,
		TEIID10081,		
	}
}
