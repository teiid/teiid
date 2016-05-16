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
package org.teiid.translator.swagger;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

public class SwaggerPlugin {
    
    public static final String PLUGIN_ID = "org.teiid.translator.swagger" ;  //$NON-NLS-1$

    private static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$
    
    public static BundleUtil Util = new BundleUtil(PLUGIN_ID, BUNDLE_NAME, ResourceBundle.getBundle(BUNDLE_NAME));
    
    public static enum Event implements BundleUtil.Event{
        TEIID28001,
        TEIID28002,
        TEIID28003,
        TEIID28004, 
        TEIID28005, 
        TEIID28006,
        TEIID28007,
        TEIID28008,
        TEIID28009,
        TEIID28010,
        TEIID28011,
        TEIID28012,
        TEIID28013,
        TEIID28014,
        TEIID28015,
        TEIID28016,
        TEIID28017,
        TEIID28018
    }   

}
