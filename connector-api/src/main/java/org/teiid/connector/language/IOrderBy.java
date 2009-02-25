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

package org.teiid.connector.language;

import java.util.List;

/**
 * Represents an ORDER BY clause in a command.  The ORDER BY is 
 * conceptually executed on the results of the SELECT clause and thus
 * specifies the columns to order by based on the names of the 
 * SELECT symbols.  So, the IOrderBy does not work on IExpression, rather 
 * it is a list of Strings matching to the values returned from 
 * {@link ISelectSymbol#getOutputName()}. 
 */
public interface IOrderBy extends ILanguageObject {

    /**
     * Get list of {@link IOrderByItem}s specifying the ordering of the results.
     * @return List of IOrderByItem
     */
    List<IOrderByItem> getItems();
    
}
