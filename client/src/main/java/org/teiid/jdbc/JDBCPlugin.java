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

package org.teiid.jdbc;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

/**
 * JDBCPlugin
 * <p>Used here in <code>jdbc</code> to have access to the new
 * logging framework.</p>
 */
public class JDBCPlugin { // extends Plugin {

    public static final String PLUGIN_ID = "org.teiid.jdbc" ; //$NON-NLS-1$

	public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,
	                                                     PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$
	public static enum Event implements BundleUtil.Event {
		TEIID20000,
		TEIID20001,
		TEIID20002,
		TEIID20003,
		TEIID20004,
		TEIID20005,
		TEIID20006,
		TEIID20007,
		TEIID20008,
		TEIID20009,
		TEIID20010,
		TEIID20011,
		TEIID20012,
		TEIID20013,
		TEIID20014,
		TEIID20015,
		TEIID20016,
		TEIID20017,
		TEIID20018,
		TEIID20019,
		TEIID20020,
		TEIID20021,
		TEIID20022,
		TEIID20023,
		TEIID20024,
		TEIID20025,
		TEIID20026,
		TEIID20027,
	}	
}
