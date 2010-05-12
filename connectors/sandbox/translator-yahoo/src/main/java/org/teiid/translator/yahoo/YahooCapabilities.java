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

package org.teiid.translator.yahoo;

import java.util.Collections;
import java.util.List;

import org.teiid.translator.BasicConnectorCapabilities;


/**
 * Specifies the capabilities of this connector.  The Yahoo connector
 * supports only some basic criteria capability
 */
public class YahooCapabilities extends BasicConnectorCapabilities {

    public static final int YAHOO_MAX_SET_SIZE = 100;
    
    /**
     * Construct the capabilities class 
     */
    public YahooCapabilities() {
    }

    public List getSupportedFunctions() {
        return Collections.EMPTY_LIST;
    }

    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    public boolean supportsInCriteria() {
        return true;
    }

    public int getMaxInCriteriaSize() {
        return YAHOO_MAX_SET_SIZE;
    }

}
