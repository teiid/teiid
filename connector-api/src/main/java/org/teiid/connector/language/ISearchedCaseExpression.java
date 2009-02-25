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
 * Represents a non-searched CASE expression:
 * <br/> CASE WHEN criteria THEN expression ... END
 */
public interface ISearchedCaseExpression extends IExpression {
    
    /**
     * Gets the number of WHEN and THEN clauses in the CASE expression
     * @return the number of WHEN ... THEN ... parts
     */
    int getWhenCount();
    
    /**
     * Gets the WHEN criteria at the specified index
     * @param index the 0-based index
     * @return  the WHEN criteria at the index
     */
    ICriteria getWhenCriteria(int index);

    /**
     * Sets the WHEN criteria at the specified index
     * @param index the 0-based index
     * @param criteria The new WHEN criteria at the index
     */
    void setWhenCriteria(int index, ICriteria criteria);
    
    /**
     * Gets the THEN expression at the specified index
     * @param index the 0-based index
     * @return  the THEN expression at the index
     */
    IExpression getThenExpression(int index);

    /**
     * Sets the THEN expression at the specified index
     * @param index the 0-based index
     * @param expression The new THEN expression at the index
     */
    void setThenExpression(int index, IExpression expression);
        
    /**
     * Gets the ELSE expression, if defined. Can be null.
     * @return the ELSE expression.
     */
    IExpression getElseExpression();
    
    /**
     * Sets the ELSE expression 
     * @param expression The new ELSE expression 
     */
    void setElseExpression(IExpression expression);
    
}
