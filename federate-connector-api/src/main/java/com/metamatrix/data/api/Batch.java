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

package com.metamatrix.data.api;

import java.util.List;

/**
 * <p>The Batch represents a portion of the results returned from a particular command
 * execution.  Because all access to the results is batched, this may contain only a
 * subset of the total rows that will be returned by an execution.</p>
 */
public interface Batch {

    /**
     * Add a row to this batch.
     * @param row One row in the set of result rows
     */
    void addRow(List row);

    /**
     * Find out how many rows are in this batch.
     * @return The count of the number of rows in this batch.
     */
    int getRowCount();

    /**
     * When adding results in the form of batches, notify that this will be the
     * last batch.  Connectors must call this method on the last batch to indicate
     * that they are doing providing results for an execution.
     */
    void setLast();

    /**
     * Determine whether this is the last batch to be expected.
     * @return <code>true</code> if this is the last batch of rows
     */
    boolean isLast();

    /**
     * Return the rows in this batch.
     * @return Array of List where each List is a row of data
     */
    List[] getResults();
}
