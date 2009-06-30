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

package com.metamatrix.common.config.api;

/**
* The ConnectorComponentType represents the connector ComponentType.
*/
public interface VMComponentDefnType extends ServiceComponentType {

    public static final String COMPONENT_TYPE_NAME = "VM"; //$NON-NLS-1$
    
    public static final String VM_MINIMUM_HEAP_SIZE_PROPERTY_NAME = org.teiid.adminapi.ProcessObject.VM_MINIMUM_HEAP_SIZE_PROPERTY_NAME;
    public static final String VM_MAXIMUM_HEAP_SIZE_PROPERTY_NAME = org.teiid.adminapi.ProcessObject.VM_MAXIMUM_HEAP_SIZE_PROPERTY_NAME;

    // Socket VM related properties
    /**
     * @see SocketVMController
     */
    public static final String CLUSTER_PORT = "vm.unicast.port"; //$NON-NLS-1$
    public static final String SERVER_PORT = org.teiid.adminapi.ProcessObject.SERVER_PORT;
    public static final String MAX_THREADS = org.teiid.adminapi.ProcessObject.MAX_THREADS;
    public static final String TIMETOLIVE = org.teiid.adminapi.ProcessObject.TIMETOLIVE;

    public static final String INPUT_BUFFER_SIZE = org.teiid.adminapi.ProcessObject.INPUT_BUFFER_SIZE;
    public static final String OUTPUT_BUFFER_SIZE = org.teiid.adminapi.ProcessObject.OUTPUT_BUFFER_SIZE;
    public static final String FORCED_SHUTDOWN_TIME = org.teiid.adminapi.ProcessObject.FORCED_SHUTDOWN_TIME;
    public static final String ENABLED_FLAG = org.teiid.adminapi.ProcessObject.ENABLED_FLAG;
    
    /**
     * When specified, indicates what address the vm will be bound to.  If this is not 
     * specfied, then the @link HostType#HOST_BIND_ADDRESS will be used.
     */
    public static final String VM_BIND_ADDRESS = "vm.bind.address"; //$NON-NLS-1$
    
    
    /**
     * Java Starter Command Properties
     * These properties are used to create the java command to execute when starting
     * the process.  These are used in conjuction with {@link HostType.JAVA_EXEC} property to 
     * complete the executable command. 
     * 
     * Example:   ${vm.start.cmd.java_opts} ${vm.starter.cmd.java_main} ${vm.starter.cmd.java_args}
     */
    
    public static final String JAVA_OPTS = "vm.starter.cmd.java_opts"; //$NON-NLS-1$
} 
