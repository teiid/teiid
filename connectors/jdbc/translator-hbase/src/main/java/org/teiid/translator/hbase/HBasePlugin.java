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
package org.teiid.translator.hbase;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

public class HBasePlugin {

    public static final String PLUGIN_ID = "org.teiid.translator.hbase" ;  //$NON-NLS-1$
    
    private static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$
    
    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID, BUNDLE_NAME, ResourceBundle.getBundle(BUNDLE_NAME));
    
    public static enum Event implements BundleUtil.Event {
        
        // Phoenix HBase Table Mapping
        TEIID27001,
        
        // HBaseQueryExecution
        TEIID27002,
        
        // HBaseUpdateExecution
        TEIID27003,
        
        // HBaseProcedureExecution
        TEIID27004,
        
        // HBaseMetadataProcessor
        TEIID27005,
        
        TEIID27006,
        TEIID27007,
        TEIID27008,
        TEIID27009,
        TEIID27010,
        
        TEIID27011,
        TEIID27012,
        TEIID27013,
        TEIID27014,
        TEIID27015,
        TEIID27016,
        TEIID27017,
        TEIID27018,
        TEIID27019,
        TEIID27020,
    }
}