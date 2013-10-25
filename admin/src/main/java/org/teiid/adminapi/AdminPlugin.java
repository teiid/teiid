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

package org.teiid.adminapi;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

public class AdminPlugin { 
    public static final String PLUGIN_ID = "org.teiid.adminapi" ; //$NON-NLS-1$
	public static final BundleUtil Util = new BundleUtil(PLUGIN_ID, PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$
	
	public static enum Event implements BundleUtil.Event {
		TEIID70000,
		TEIID70003,
		TEIID70004,
		TEIID70005,
		TEIID70006,
		TEIID70007,
		TEIID70008,
		TEIID70009,
		TEIID70010,
		TEIID70011,
		TEIID70013,
		TEIID70014,
		TEIID70015,
		TEIID70016,
		TEIID70020,
		TEIID70021,
		TEIID70022,
		TEIID70023,
		TEIID70024,
		TEIID70025,
		TEIID70026,
		TEIID70027,
		TEIID70028,
		TEIID70029,
		TEIID70030,
		TEIID70031,
		TEIID70032,
		TEIID70033,
		TEIID70034,
		TEIID70035,
		TEIID70036,
		TEIID70037,
		TEIID70038,
		TEIID70039,
		TEIID70040,
		TEIID70041,
		TEIID70042,
		TEIID70043,
		TEIID70044,
		TEIID70045,
		TEIID70046,
		TEIID70047,
		TEIID70048,
		TEIID70049,
		TEIID70050,	
		TEIID70051,
		TEIID70052, 
		TEIID70053, 
		TEIID70054,
		TEIID70055
	}
}
