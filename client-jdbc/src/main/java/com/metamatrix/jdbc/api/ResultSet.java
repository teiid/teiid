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

package com.metamatrix.jdbc.api;

import java.sql.SQLException;
import java.util.Date;

/**
 * The MetaMatrix-specific interface for using result sets from JDBC.  This 
 * interface provides some additional MetaMatrix-specific functionality that 
 * is not available through the standard JDBC interfaces.  
 */
public interface ResultSet extends java.sql.ResultSet {

    /**
     * Return the timestamp when the command was submitted to the server.
     * @return Date object representing time submitted to the server.
     */
    Date getProcessingTimestamp() throws SQLException;

    /**
     * Return the time the command execution completed on the server.
     * @return Date object representing time the command finished execution.
     */
    Date getCompletedTimestamp() throws SQLException;

    /**
     * Return the elapsed processing time on the server for this command.  
     * @return Elapsed time in milliseconds
     */
    long getProcessingTime() throws SQLException;
    
}
