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
 * Represents a quantified comparison criteria.  This criteria has an expression on the left,
 * a comparison operator (such as =, &lt;, etc), a quantification operator (ALL, ANY), 
 * and a subquery.
 */
public interface ISubqueryCompareCriteria extends IPredicateCriteria, ISubqueryContainer {

	public enum Quantifier {
		SOME,
		ALL
	}
    /**
     * Get left expression.
     * @return Left expression
     */
    IExpression getLeftExpression();

    /**
     * Get operator from set defined in this interface.
     * @return Operator constant
     * @see #EQ
     * @see #NE
     * @see #LT
     * @see #LE
     * @see #GT
     * @see #GE
     */
    ICompareCriteria.Operator getOperator();

    /**
     * Get quantifier.
     * @return Quantifier constant
     * @see Quantifier#SOME
     * @see Quantifier#ALL
     */
    Quantifier getQuantifier();
    
    /**
     * Set left expression.
     * @param expression Left expression
     */
    void setLeftExpression(IExpression expression);

    /**
     * Set operator from set defined in this interface.
     * @param quantifier Operator constant
     * @see #EQ
     * @see #NE
     * @see #LT
     * @see #LE
     * @see #GT
     * @see #GE
     */
    void setOperator(ICompareCriteria.Operator operator);

    /**
     * Set quantifier.
     * @param quantifier Quantifier constant
     * @see Quantifier#SOME
     * @see Quantifier#ALL
     */
    void setQuantifier(Quantifier quantifier);
    

}
