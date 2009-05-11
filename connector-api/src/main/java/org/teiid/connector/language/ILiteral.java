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

/**
 * Represents a literal value that is used in
 * an expression.  The value can be obtained and should match
 * the type specified by {@link ILiteral#getType}.
 */
public interface ILiteral extends IExpression {
    /**
     * Get the value of the literal 
     * @return Object of value
     */
    Object getValue();
    
    /**
     * Get the Java type of the literal
     * @return Java class name of type
     */
    Class<?> getType();  
    
    /**
     * Set the value of the literal 
     * @param value Object of value
     */
    void setValue(Object value);
    
    /**
     * Set the Java type of the literal
     * @param type Java class name of type
     */
    void setType(Class<?> type);
    
    /**
     * Returns true if this literal should be treated as a bind value
     */
    boolean isBindValue();
    
    /**
     * Set whether this literal should be treated as a bind value
     * @param bindValue
     */
    void setBindValue(boolean bindValue);
    
    /**
     * Returns true if the value for this literal is a list of values.
     * @return
     */
    boolean isMultiValued();
    
    /**
     * Set whether the value for this literal is a list of values.
     * @param multiValued
     */
    void setMultiValued(boolean multiValued);
     
}
