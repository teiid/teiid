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

import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ICommand;


/** 
 * Classes implementing this interface will provide support for batched updates.
 * @since 4.2
 */
public interface BatchedUpdatesExecution extends Execution {
    /**
     * Execute the given batch of commands and return the update count for each command.
     * @param commands an ordered array of ICommands representing the batch to be executed.
     * @return an array of update counts containing one element for each command in the batch.
     * The elements of the array are ordered in the same order as the given batch
     * @throws ConnectorException
     * @since 4.2
     */
    int[] execute(ICommand[] commands) throws ConnectorException;
}
