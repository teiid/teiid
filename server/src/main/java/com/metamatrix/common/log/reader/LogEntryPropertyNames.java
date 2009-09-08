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

package com.metamatrix.common.log.reader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;



/** 
 * @since 4.3
 */
public class LogEntryPropertyNames implements Serializable {

   
    /**
     * The name of the System property that contains the maximum number of rows
     * that will be returned for viewing..  This is an optional property that defaults to '2500'.
     */
    public static final String MAX_LOG_ROWS_RETURNED = "metamatrix.log.maxRows"; //$NON-NLS-1$

    /**
     * List of String database column names which store the
     * log message attributes
     */
    public static List COLUMN_NAMES;

    
    static {
        COLUMN_NAMES = new ArrayList();        
        COLUMN_NAMES.add(ColumnName.CONTEXT);
        COLUMN_NAMES.add(ColumnName.LEVEL);
        COLUMN_NAMES.add(ColumnName.MESSAGE);
        COLUMN_NAMES.add(ColumnName.EXCEPTION);
        COLUMN_NAMES.add(ColumnName.TIMESTAMP);
        COLUMN_NAMES.add(ColumnName.HOST);
        COLUMN_NAMES.add(ColumnName.VM);
        COLUMN_NAMES.add(ColumnName.THREAD);
        COLUMN_NAMES = Collections.unmodifiableList(COLUMN_NAMES);       
       
    }

    
    public static final class ColumnName {
        public static final String TIMESTAMP        = "TIMESTAMP"; //$NON-NLS-1$
        public static final String SEQUENCE_NUMBER  = "VMSEQNUM"; //$NON-NLS-1$
        public static final String CONTEXT          = "CONTEXT"; //$NON-NLS-1$
        public static final String LEVEL            = "MSGLEVEL"; //$NON-NLS-1$
        public static final String EXCEPTION        = "EXCEPTION"; //$NON-NLS-1$
        public static final String MESSAGE          = "MESSAGE"; //$NON-NLS-1$
        public static final String HOST             = "HOSTNAME"; //$NON-NLS-1$
        public static final String VM               = "VMID"; //$NON-NLS-1$
        public static final String THREAD           = "THREADNAME"; //$NON-NLS-1$
    }

    
    
}
