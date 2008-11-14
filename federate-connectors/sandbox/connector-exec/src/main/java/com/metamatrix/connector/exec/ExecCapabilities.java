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

package com.metamatrix.connector.exec;
import java.util.Collections;
import java.util.List;

import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.basic.BasicConnectorCapabilities;

/**
 */
public class ExecCapabilities extends BasicConnectorCapabilities {

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsExecutionMode(int)
     */
    public boolean supportsExecutionMode(int executionMode) {
        return executionMode == ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#getSupportedFunctions()
     */
    public List getSupportedFunctions() {
        return Collections.EMPTY_LIST;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsOrCriteria()
     */
    public boolean supportsOrCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsCompareCriteria()
     */
    public boolean supportsCompareCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsCompareCriteriaEquals()
     */
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsCriteria()
     */
    public boolean supportsCriteria() {
        return true;
    }

    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsInCriteria()
     */
    public boolean supportsInCriteria() {
        return true;
    }


}
