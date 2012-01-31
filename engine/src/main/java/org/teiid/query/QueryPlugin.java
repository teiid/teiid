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

package org.teiid.query;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

/**
 * QueryPlugin
 * <p>
 * Used here in <code>query</code> to have access to the new logging framework for <code>LogManager</code>.
 * </p>
 */
public class QueryPlugin { // extends Plugin {

	/**
	 * The plug-in identifier of this plugin 
	 */
	public static final String PLUGIN_ID = QueryPlugin.class.getPackage().getName();

	public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,
	                                                     PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$
	
	
	public static enum Event implements BundleUtil.Event{
		TEIID30001, // buffer manager max block exceeded
		TEIID30002, // error persisting buffer manager
		TEIID30003, // capability required
		TEIID30004, // zero size batch
		TEIID30005, // rollback failed
		TEIID30006, // invalid max active plans
		TEIID30007, // general process worker error
		TEIID30008, // request not deterministic
		TEIID30009, // max threads exceeded
		TEIID30010, // duplicate function
		TEIID30011, // dependent criteria over max
		TEIID30012, // mat row refresh
		TEIID30013, // mat table loading
		TEIID30014, // mat table loaded
		TEIID30015, //faild to load mat table
		TEIID30016, // error transfer
		TEIID30017, // error persisting batch in bm
		TEIID30018, 
		TEIID30019, // process worker error
		TEIID30020, // process worker error
		TEIID30021, // uncaught exception during work
		TEIID30022, // error defrag
		TEIID30023, // error defrag truncate
		TEIID30024, // cancel request failed
		TEIID30025, // failed to restore results
		TEIID30026, // failed to cancel
		TEIID30027, // lob error
		TEIID30028, // failed to rollback
		TEIID30029, // unexpected format
		TEIID30030, // unexpected exp1
		TEIID30031, // unexpected exp2
		TEIID30032, // invalid collation locale
		TEIID30033, // using collation locale
	}
}
