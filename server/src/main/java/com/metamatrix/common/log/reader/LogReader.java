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

package com.metamatrix.common.log.reader;

import java.util.Date;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;

/**
 * Class that reads MetaMatrix log entries.
 */
public interface LogReader {

   

    /** 
     * @see com.metamatrix.platform.admin.api.RuntimeStateAdminAPI#getLogEntries(java.util.Date, java.util.Date, java.util.List, java.util.List, int)
     * @since 4.3
     */
    List getLogEntries(Date startTime,
                       Date endTime,
                       List levels,
                       List contexts,
                       int maxRows) throws MetaMatrixComponentException;
   
      
}

