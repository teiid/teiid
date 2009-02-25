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

import org.teiid.connector.metadata.runtime.Parameter;

/**
 * Represents an execution parameter.
 */
public interface IParameter extends ILanguageObject, IMetadataReference<Parameter> {
	
	public enum Direction {
		IN,
		OUT,
		INOUT,
		RETURN,
		RESULT_SET
	}

    /**
     * Get index of this parameter in the IExecution.
     * @return Index of the parameter
     */
    int getIndex();
    
    /**
     * Get direction of parameter
     * @return Direction
     * @see Direction#IN
     * @see Direction#OUT
     * @see Direction#INOUT
     * @see Direction#RETURN
     */
    Direction getDirection();
    
    /**
     * Get type of parameter, defined as a Java class.  Output result sets will
     * return "java.sql.ResultSet".  
     * @return Type of parameter
     */
    Class getType();    
    
    /**
     * Get value of object if this is an IN parameter.  
     * @return Value of IN parameter or null otherwise
     */
    Object getValue();

    /**
     * Determine whether a value was set for this parameter.  This will only
     * return true if this parameter is an IN or INOUT parameter and a value was 
     * set by the caller.  If no value was set, then this parameter was determined
     * to be optional and has no default value.  Connectors may deal with this 
     * optional parameter as necessary, perhaps by using named parameters when 
     * executing the procedure to avoid unspecified inputs.
     *   
     * @return True if IN or INOUT and no value was set
     * @since 4.3.2
     */
    boolean getValueSpecified();
    
    /**
     * Set index of this parameter in the IExecution.
     * @param index Index of the parameter
     */
    void setIndex(int index);
    
    /**
     * Set direction of parameter
     * @param direction Direction
     * @see Direction#IN
     * @see Direction#OUT
     * @see Direction#INOUT
     * @see Direction#RETURN
     */
    void setDirection(Direction direction);
    
    /**
     * Set type of parameter, defined as a Java class.  Output result sets will
     * return "java.sql.ResultSet".  
     * @param type Type of parameter
     */
    void setType(Class type);    
    
    /**
     * Set value of object if this is an IN parameter.  If
     * value is non-null, the valueSpecified attribute will be 
     * automatically set.  If null, callers must also call {@link #setValueSpecified(boolean)}
     * to indicate whether the null was a missing or actual value.
     * 
     * @param value Value of IN parameter or null otherwise
     */
    void setValue(Object value);

    /**
     * Indicate that a value was specified for this parameter.  If no 
     * value was specified, the connector must deal with the optional parameter
     * as necessary.
     *   
     * @param specified True if value was specified 
     * @since 4.3.2
     */
    void setValueSpecified(boolean specified);
}
