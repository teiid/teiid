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
package org.teiid.rhq.comm.impl;

import org.teiid.adminapi.AdminObject;


/** 
 * @since 4.3
 */
public interface TeiidConnectionConstants {

    public static final String MMPORT = "mmport"; //$NON-NLS-1$
    public static final String PORT_DELIM = ","; //$NON-NLS-1$
   
    
    public static String ID_DELIMITER = AdminObject.DELIMITER;
    public static String MMPROCESS = AdminObject.DELIMITER + "MMProcess"; //$NON-NLS-1$
    public static String WILDCARD = "*"; //$NON-NLS-1$
    public static final String LONGRUNNINGQUERYTIME = "longRunningQueryTime"; //$NON-NLS-1$
    public static final double MS_TO_MIN = 1.66666666666667E-05;
    public static final String PROTOCOL = "mm://"; //$NON-NLS-1$
    
    public static final String SSL_PROTOCOL = "mms://"; //$NON-NLS-1$   
    
    public static String defaultPort = "31000"; //$NON-NLS-1$
    
    public static String SYSTEM_NAME_PROPERTY = "metamatrix.cluster.name"; //$NON-NLS-1$

}
