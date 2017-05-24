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
package org.teiid.translator.couchbase;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

public class CouchbasePlugin {

    public static final String PLUGIN_ID = "org.teiid.translator.couchbase" ; //$NON-NLS-1$

    private static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$
    
    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,BUNDLE_NAME,ResourceBundle.getBundle(BUNDLE_NAME));

    public static enum Event implements BundleUtil.Event{
        TEIID29001,
        TEIID29002,
        TEIID29003,
        TEIID29004,
        TEIID29005,
        TEIID29006,
        TEIID29007,
        TEIID29008,
        TEIID29009,
        TEIID29010,
        TEIID29011,
        TEIID29012,
        TEIID29013,
        TEIID29014,
        TEIID29015,
        TEIID29016,
        TEIID29017,
        TEIID29018,
        TEIID29019,
        TEIID29020,
        TEIID29021
    }
}
