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

package org.teiid.translator.excel;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;


public class ExcelPlugin { 
    public static final String PLUGIN_ID = "org.teiid.translator.excel" ; //$NON-NLS-1$
	public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$
	
	public static enum Event implements BundleUtil.Event{
		TEIID23000,
		TEIID23001,
		TEIID23002,
		TEIID23003,
		TEIID23004,
		TEIID23005,
		TEIID23006,
		TEIID23007,
		TEIID23008,
		TEIID23009,
		TEIID23010
	}
}
