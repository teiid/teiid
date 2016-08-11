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
package org.teiid.olingo;

import java.util.Locale;
import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;


public class ODataPlugin {
    private static final String PLUGIN_ID = "org.teiid.olingo" ; //$NON-NLS-1$
    static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$
    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,BUNDLE_NAME,ResourceBundle.getBundle(BUNDLE_NAME));

    public static ResourceBundle getResourceBundle(Locale locale) {
        if (locale == null) {
            locale = Locale.getDefault();
        }
        return ResourceBundle.getBundle(ODataPlugin.BUNDLE_NAME, locale);
    }

    public static enum Event implements BundleUtil.Event {
        TEIID16001,
        TEIID16002,
        TEIID16003,
        TEIID16004,
        TEIID16005,
        TEIID16006,
        TEIID16007,
        TEIID16008,
        TEIID16009,
        TEIID16010,
        TEIID16011, 
        TEIID16012,
        TEIID16013, 
        TEIID16014,
        TEIID16015,
        TEIID16016,
        TEIID16017,
        TEIID16018,
        TEIID16019,
        TEIID16020,
        TEIID16021,
        TEIID16022,
        TEIID16023,
        TEIID16024,
        TEIID16025,
        TEIID16026,
        TEIID16027,
        TEIID16028,
        TEIID16029,
        TEIID16030,
        TEIID16031,
        TEIID16032,
        TEIID16033,
        TEIID16034,
        TEIID16035,
        TEIID16036,
        TEIID16037,
        TEIID16038,
        TEIID16039,
        TEIID16040,
        TEIID16041,
        TEIID16042,
        TEIID16043, 
        TEIID16044, 
        TEIID16045,
        TEIID16046, 
        TEIID16047, 
        TEIID16048,
        TEIID16049, 
        TEIID16050,
        TEIID16051, 
        TEIID16052,
        TEIID16053,
        TEIID16054,
        TEIID16055,
        TEIID16056
    }
}
