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

package com.metamatrix.admin.api.server;

import java.util.Collection;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminRoles;
import org.teiid.adminapi.MonitoringAdmin;

import com.metamatrix.admin.RolesAllowed;


/**
 * Interface that exposes the MetaMatrix server for system monitoring.
 *
 * <p>Clients should <i>not</i> code directly to this interface but
 * should instead use {@link ServerAdmin}.</p>
 * @since 4.3
 */
@RolesAllowed(value=AdminRoles.RoleName.ADMIN_READONLY)
public interface ServerMonitoringAdmin extends MonitoringAdmin {

    /**
     * Get the hosts that correspond to the specified identifier pattern.
     *
     * @param hostIdentifier the unique identifier for for a {@link org.teiid.adminapi.Host Host}
     * in the system or "{@link org.teiid.adminapi.AdminObject#WILDCARD WILDCARD}"
     * if all hosts are desired.
     * @return Collection of {@link org.teiid.adminapi.Host Host}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    Collection getHosts(String hostIdentifier) throws AdminException;

    /**
     * Get the processes that correspond to the specified identifier pattern.
     *
     * @param processIdentifier the unique identifier for for a {@link org.teiid.adminapi.ProcessObject ProcessObject}
     * in the system or "{@link org.teiid.adminapi.AdminObject#WILDCARD WILDCARD}"
     * if all Processes are desired.
     * @return Collection of {@link org.teiid.adminapi.ProcessObject ProcessObject}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    @RolesAllowed(value=AdminRoles.RoleName.ANONYMOUS)
    Collection getProcesses(String processIdentifier) throws AdminException;
    
    
    /**
     * Get the Services that correspond to the specified identifer pattern.
     * These services represent what is defined for a given {@link Host} | {@link Process};
     *
     * @param resourceIdentifier the unique identifier for for a {@link org.teiid.adminapi.Service Service}
     * in the system or "{@link org.teiid.adminapi.AdminObject#WILDCARD WILDCARD}"
     * if all Services are desired.
     * @return Collection of {@link org.teiid.adminapi.Service Service}
     * @throws AdminException if there's a system error.
     * @since 6.1
     */

    Collection getServices(String identifier) throws AdminException ;


    /**
     * Get the Resources that correspond to the specified identifer pattern.
     *
     * @param resourceIdentifier the unique resourceIdentifier for for a {@link org.teiid.adminapi.Resource Resource}
     * in the system or "{@link org.teiid.adminapi.AdminObject#WILDCARD WILDCARD}"
     * if all Resources are desired.
     * @return Collection of {@link org.teiid.adminapi.Resource Resource}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    Collection getResources(String resourceIdentifier) throws AdminException;

    /**
     * Get the {@link org.teiid.adminapi.DQP DQP}s that correspond to the specified identifer pattern.
     *
     * @param identifier the unique identifier for for a {@link org.teiid.adminapi.DQP DQP}
     * in the system or "{@link org.teiid.adminapi.AdminObject#WILDCARD WILDCARD}"
     * if all Resources are desired.
     * @return Collection of {@link org.teiid.adminapi.DQP DQP}
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    Collection getDQPs(String identifier) throws AdminException;
    
    /**
     * Export the server logs to a byte[].  The bytes contain the contents of a .zip file containing the logs. 
     * @return the logs, as a byte[].
     * @throws AdminException
     * @since 4.3
     */
    byte[] exportLogs() throws AdminException;


}
