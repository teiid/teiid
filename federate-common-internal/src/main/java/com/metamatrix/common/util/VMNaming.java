/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.util;

import java.util.Random;

import com.metamatrix.core.util.HashCodeUtil;

public final class VMNaming {

    public static final String VM_NAME_PROPERTY = "metamatrix.vmname"; //$NON-NLS-1$

    private static int VM_ID = 0;
    private static int MAX_VM_ID = 9999;
    
    /* 
     * LOGICAL_HOSTNAME refers to the hostname used to look up the host in the configuration
     */
    private static String LOGICAL_HOSTNAME = "";//$NON-NLS-1$

    /*
     * HOST_ADDRESS refers to the physical host address used when opening the socket
     * This may or maynot be the same as the #LOGICAL_HOSTNAME
     */
    private static String HOST_ADDRESS = "";//$NON-NLS-1$
    
    /*
     * BIND_ADDRESS refers to the address used by listeners.  This would include
     * the socket listeners and JGroups.
     * This may or maynot be the same as the #LOGICAL_HOSTNAME
     */
    private static String BIND_ADDRESS = "";//$NON-NLS-1$
    
    
    private static String VMNAME = System.getProperty(VM_NAME_PROPERTY, "");//$NON-NLS-1$

   static {
        String hostName = ""; //$NON-NLS-1$
        try {
            hostName = NetUtils.getHostname();
            
            LOGICAL_HOSTNAME = hostName;
            BIND_ADDRESS = hostName;
            HOST_ADDRESS = hostName;
        } catch ( Exception e ) {
        }
        
        // Compute a "pseudo-unique" VM ID
        int seed = 10;
        seed = HashCodeUtil.hashCode(seed,Runtime.getRuntime().hashCode());
        seed = HashCodeUtil.hashCode(seed,hostName);
        seed = HashCodeUtil.hashCode(seed,System.currentTimeMillis());

        Random randomGenerator = new Random(seed);
        VM_ID = randomGenerator.nextInt(MAX_VM_ID);
    }

        

    public static String getVMName() {
        return VMNAME;
    }
    
    public static String getLogicalHostName() {
        return LOGICAL_HOSTNAME;
    }
    
    
    public static String getHostAddress() {
        return HOST_ADDRESS;
    }
    
    public static String getBindAddress() {
        return BIND_ADDRESS;
    }     

    public static int getVMID() {
        return VM_ID;
    }

    public static String getVMIDString() {
        return String.valueOf(VM_ID);
    }
    
    public static void setLogicalHostName(String hostName) {
        LOGICAL_HOSTNAME = hostName;
    }    
    
    public static void setHostAddress(String hostAddress) {
        if (hostAddress != null) {
            NetUtils.setHostName(hostAddress);
        
            HOST_ADDRESS = hostAddress;
        }
    }
    
    public static void setBindAddress(String bindaddress) {
        BIND_ADDRESS = bindaddress;
    }     

}
