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

package com.metamatrix.query.sql.lang;


/**
 * This abstract class represents an atomic logical criteria.  An
 * atomic criteria operates on a single other criteria and evaluates
 * to true or false during processing.
 */
public abstract class AtomicCriteria extends LogicalCriteria {

	/** The single sub criteria */
	private Criteria criteria;
	
	/**
	 * Constructs a default instance of this class.
	 */
	protected AtomicCriteria() {
	}

	/**
	 * Constructs an instance of this class with a single sub-criteria.
	 */
	protected AtomicCriteria(Criteria crit) {
		setCriteria(crit);
	}

	/**
	 * Get sub criteria
	 * @return Sub criteria
	 */
	public Criteria getCriteria() {
		return criteria;
	}

	/**
	 * Set sub criteria
	 * @param criteria Sub criteria
	 */
	public void setCriteria(Criteria criteria) {
		this.criteria = criteria;
	}

	/**
	 * Deep copy of object
	 * @return Deep copy of object
	 */
	public abstract Object clone();

}

