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
 */
public interface IBaseInCriteria extends IPredicateCriteria {
    /**
     * Get left expression of IN criteria
     * @return Left expression
     */
    IExpression getLeftExpression();

    /**
     * Set left expression of IN criteria
     * @return Left expression
     */
    void setLeftExpression(IExpression expression);

    /**
     * Returns whether this criteria is negated.
     * @return flag indicating whether this criteria contains a NOT
     */
    boolean isNegated();
    
    /**
     * Sets whether this criteria is negated.
     * @param negated Flag indicating whether this criteria contains a NOT
     */
    void setNegated(boolean negated);
    
}
