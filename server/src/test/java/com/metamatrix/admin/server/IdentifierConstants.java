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

package com.metamatrix.admin.server;

import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.Request;


/** 
 * @since 4.3
 */
public interface IdentifierConstants {

    /** 
     * @since 4.3
     */
    final String _1_1_WILDCARD = "1" + AdminObject.DELIMITER + "1" + AdminObject.WILDCARD; //$NON-NLS-1$ //$NON-NLS-2$
    /** 
     * @since 4.3
     */
    final String _1_WILDCARD = "1" + AdminObject.WILDCARD; //$NON-NLS-1$
    /** 
     * Host names may be fully qualified (contain '.' chars)
     * @since 4.3
     */
    final String HOST_3_3_3_3 = "3.3.3.3"; //$NON-NLS-1$
    /** 
     * Host names may be fully qualified (contain '.' chars)
     * @since 4.3
     */
    final String HOST_2_2_2_2 = "2.2.2.2"; //$NON-NLS-1$
    /** 
     * Host names may be fully qualified (contain '.' chars)
     * @since 4.3
     */
    final String HOST_1_1_1_1 = "1.1.1.1"; //$NON-NLS-1$
    /** 
     * @since 4.3
     */
    final String REQUEST_1_1 = "1" + Request.DELIMITER + "1"; //$NON-NLS-1$ //$NON-NLS-2$
    /** 
     * @since 4.3
     */
    final String REQUEST_1_1_1_0 = "1" + Request.DELIMITER + "1" + Request.DELIMITER + "1" + Request.DELIMITER + "0"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$  
    /** 
     * @since 4.3
     */
    final String HOST_3_3_3_3_PROCESS3_CONNECTOR_DQP3_POOL = HOST_3_3_3_3 + AdminObject.DELIMITER + "process3" + AdminObject.DELIMITER + "dqp3" + AdminObject.DELIMITER + "pool"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
    /** 
     * @since 4.3
     */
    final String HOST_2_2_2_2_PROCESS2_CONNECTOR_DQP2_POOL = HOST_2_2_2_2 + AdminObject.DELIMITER + "process2" + AdminObject.DELIMITER + "dqp2" + AdminObject.DELIMITER + "pool"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
    /** 
     * @since 4.3
     */
    final String HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2_POOL = HOST_2_2_2_2 + AdminObject.DELIMITER + "process2" + AdminObject.DELIMITER + "connectorBinding2" + AdminObject.DELIMITER + "pool"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
    /** 
     * @since 4.3
     */
    final String HOST_3_3_3_3_PROCESS3_CONNECTOR_BINDING3_POOL = HOST_3_3_3_3 + AdminObject.DELIMITER + "process3" + AdminObject.DELIMITER + "connectorBinding3" + AdminObject.DELIMITER + "pool"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
    /** 
     * @since 4.3
     */
    final String HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2_WILDCARD = HOST_2_2_2_2+ AdminObject.DELIMITER + "process2" + AdminObject.DELIMITER + "connectorBinding2" + AdminObject.WILDCARD; //$NON-NLS-1$ //$NON-NLS-2$
    /** 
     * @since 4.3
     */
    final String HOST_3_3_3_3_PROCESS3 = HOST_3_3_3_3 + AdminObject.DELIMITER + "process3"; //$NON-NLS-1$
    /** 
     * @since 4.3
     */
    final String HOST_2_2_2_2_PROCESS2_POOL = HOST_2_2_2_2 + AdminObject.DELIMITER + "process2" + AdminObject.DELIMITER + "pool"; //$NON-NLS-1$ //$NON-NLS-2$ 
    /** 
     * @since 4.3
     */
    final String HOST_2_2_2_2_PROCESS2 = HOST_2_2_2_2 + AdminObject.DELIMITER + "process2"; //$NON-NLS-1$ 
    /** 
     * @since 4.3
     */
    final String HOST_1_1_1_1_PROCESS1 = HOST_1_1_1_1 + AdminObject.DELIMITER + "process1"; //$NON-NLS-1$ 
    /** 
     * @since 4.3
     */
    final String _3_3_3_3_PROCESS3_DQP3 = HOST_3_3_3_3 + AdminObject.DELIMITER + "process3" + AdminObject.DELIMITER + "dqp3"; //$NON-NLS-1$ //$NON-NLS-2$ 
    /** 
     * @since 4.3
     */
    final String HOST_2_2_2_2_PROCESS2_DQP2 = HOST_2_2_2_2 + AdminObject.DELIMITER + "process2" + AdminObject.DELIMITER + "dqp2"; //$NON-NLS-1$ //$NON-NLS-2$ 
    /** 
     * @since 4.3
     */
    final String HOST_1_1_1_1_PROCESS1_DQP1 = HOST_1_1_1_1 + AdminObject.DELIMITER + "process1" + AdminObject.DELIMITER + "dqp1"; //$NON-NLS-1$ //$NON-NLS-2$ 
    /** 
     * @since 4.3
     */
    final String HOST_2_2_2_2_WILDCARD = HOST_2_2_2_2 + AdminObject.WILDCARD;
    /** 
     * @since 4.3
     */
    final String HOST_2_2_2_2_PROCESS2_WILDCARD = HOST_2_2_2_2 + AdminObject.DELIMITER + "process2" + AdminObject.WILDCARD; //$NON-NLS-1$ 
    /** 
     * @since 4.3
     */
    final String _3_3_3_3_PROCESS3_CONNECTOR_BINDING3 = HOST_3_3_3_3 + AdminObject.DELIMITER + "process3" + AdminObject.DELIMITER + "connectorBinding3"; //$NON-NLS-1$ //$NON-NLS-2$ 
    /** 
     * @since 4.3
     */
    final String HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2 = HOST_2_2_2_2 + AdminObject.DELIMITER + "process2" + AdminObject.DELIMITER + "connectorBinding2"; //$NON-NLS-1$ //$NON-NLS-2$ 
    /** 
     * @since 4.3
     */
    final String HOST_1_1_1_1_PROCESS1_CONNECTOR_BINDING1 = HOST_1_1_1_1 + AdminObject.DELIMITER + "process1" + AdminObject.DELIMITER + "connectorBinding1"; //$NON-NLS-1$ //$NON-NLS-2$ 
    /** 
     * @since 4.3
     */
    final String HOST_1_1_1_1_PROCESS1_WILDCARD = HOST_1_1_1_1 + AdminObject.DELIMITER + "process1" + AdminObject.WILDCARD; //$NON-NLS-1$ 
    /** 
     * @since 4.3
     */
    final String HOST_1_1_1_1_WILDCARD = HOST_1_1_1_1 + AdminObject.WILDCARD;
    /** 
     * @since 4.3
     */
    final String HOST_2_2_2_2_PROCESS2_POOL1 = HOST_2_2_2_2 + AdminObject.DELIMITER + "process2" + AdminObject.DELIMITER + "pool1"; //$NON-NLS-1$ //$NON-NLS-2$
    /** 
     * @since 4.3
     */
    final String HOST_1_1_1_1_PROCESS1_POOL2 = HOST_1_1_1_1 + AdminObject.DELIMITER + "process1" + AdminObject.DELIMITER + "pool2"; //$NON-NLS-1$ //$NON-NLS-2$
    /** 
     * @since 4.3
     */
    final String HOST_2_2_2_2_PROCESS2_POOL2 = HOST_2_2_2_2 + AdminObject.DELIMITER + "process2" + AdminObject.DELIMITER + "pool2"; //$NON-NLS-1$ //$NON-NLS-2$
    /** 
     * @since 4.3
     */
    final String HOST_1_1_1_1_PROCESS1_POOL1 = HOST_1_1_1_1 + AdminObject.DELIMITER + "process1" + AdminObject.DELIMITER + "pool1"; //$NON-NLS-1$ //$NON-NLS-2$ 
}
