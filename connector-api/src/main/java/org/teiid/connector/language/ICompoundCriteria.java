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
 * Represents a compound logical criteria such as AND or OR. 
 */
public interface ICompoundCriteria extends ILogicalCriteria {

	public enum Operator {
		AND,
		OR
	}
	
    /**
     * Get operator used to connect these criteria.
     * @return Operator constant
     * @see Operator#AND
     * @see Operator#OR
     */
    Operator getOperator();

    /**
     * Set operator used to connect these criteria.
     * @param operator Operator constant
     * @see Operator#AND
     * @see Operator#OR
     */
    void setOperator(Operator operator);

    /**
     * Get list of ICriteria combined by this compound criteria.
     * @return List of ICriteria
     */
    List<ICriteria> getCriteria();
    
}
