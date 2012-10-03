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

package org.teiid.connector;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

public class DataPlugin { 

    public static final String PLUGIN_ID = DataPlugin.class.getPackage().getName(); 

	public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,
	                                                     PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$

	public static enum Event implements BundleUtil.Event {
		TEIID60000,
		TEIID60001,
		TEIID60004,
		TEIID60005,
		TEIID60008,
		TEIID60009,
		TEIID60010,
		TEIID60011,	
		TEIID60012,
		TEIID60013,
		TEIID60014,
		TEIID60015, 
		TEIID60016, 
		TEIID60017, 
		TEIID60018
	}
}
