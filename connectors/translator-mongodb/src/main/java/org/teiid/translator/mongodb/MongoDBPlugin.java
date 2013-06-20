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
package org.teiid.translator.mongodb;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;


public class MongoDBPlugin {

    public static final String PLUGIN_ID = "org.teiid.translator.mongodb" ; //$NON-NLS-1$

    private static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$
    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,BUNDLE_NAME,ResourceBundle.getBundle(BUNDLE_NAME));

    public static enum Event implements BundleUtil.Event{
    	TEIID18001,
    	TEIID18002,
    	TEIID18003,
    	TEIID18004,
    	TEIID18005,
    	TEIID18006,
    	TEIID18007,
    	TEIID18008,
    	TEIID18009,
    	TEIID18010,
    	TEIID18011,
    	TEIID18012,
    	TEIID18013,
    	TEIID18014
    }
}
