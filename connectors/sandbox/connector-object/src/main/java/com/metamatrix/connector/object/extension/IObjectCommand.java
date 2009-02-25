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

package com.metamatrix.connector.object.extension;

import java.util.List;
import java.util.Map;

import org.teiid.connector.language.ICommand;



/** 
 * The IObjectCommand is the command to be executed.  This indicates what is being
 * executed and  This also contains the metadata about
 * that command.
 * @since 4.3
 */
public interface IObjectCommand {
    
    /**
     * Returns the command being executed. 
     * @return
     * @since 4.3
     */
    ICommand getCommand();
    
    /**
     * Returns the table name or procedure that that will be used to execute on 
     * @return
     * @since 4.3
     */
    String getGroupName();
    
    /**
     * Returns the table name or procedure that that will be used to execute on 
     * @return
     * @since 4.3
     */
    String getGroupNameInSource();    
        
    /**
     * Returns a list of Classes for the different criteria. 
     * @return
     * @since 4.3
     */
    List getCriteriaTypes();
    
    /**
     * Returns the criteria values. 
     * @return
     * @since 4.3
     */
    List getCriteriaValues();
    
    /**
     * Returns name value pairs which provide the search criteria. 
     * @return
     * @since 4.3
     */
    Map getCriteria();
    
    /**
     * Returns the column names in the result set 
     * @return
     * @since 4.3
     */
    String[] getResultColumnNames();
    
    /**
     * Returns the nameInSource for each column.  If a nameInSource
     * was not specified, it will be null.
     * @return
     * @since 4.3
     */
    String[] getResultNamesInSource();   
    
    /**
     * Return the Class type for each result column 
     * @return
     * @since 4.3
     */
    Class[] getResultColumnTypes();
    
    /**
     * Returns true if the command may have results. 
     * @return
     * @since 4.3
     */
    boolean hasResults() ;
    
    
}
