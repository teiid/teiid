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
package org.teiid.rhq;



/** 
 * @since 4.3
 */
public interface StartingEnvironmentConstants {

    public static final String INSTALL_DIR = "D:/apps/MetaMatrix/enterpriseserver/5.5.3/073008"; //$NON-NLS-1$
    public static final String PORT = "31000"; //$NON-NLS-1$
    public static final String USERNAME = "metamatrixadmin"; //$NON-NLS-1$
    public static final String PASSWORD = "mm"; //$NON-NLS-1$
    
    public static final String INSTALL_DIR2 = "D:/metamatrix/5.5.3/server_0721b"; //$NON-NLS-1$
    public static final String PORT2 = "32000"; //$NON-NLS-1$
    
    
    
    
    public static final String SINGLE_SYSTEM_PARM =  
                INSTALL_DIR + "," + //$NON-NLS-1$
                PORT + "," + //$NON-NLS-1$
                USERNAME + "," + //$NON-NLS-1$
                PASSWORD;
                
    public static final String TWO_SYSTEM_PARM = SINGLE_SYSTEM_PARM + ";" + //$NON-NLS-1$
                INSTALL_DIR2 + "," + //$NON-NLS-1$
                PORT2 + "," + //$NON-NLS-1$
                USERNAME + "," + //$NON-NLS-1$
                PASSWORD;
            
    

}
