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

package org.teiid.resource.adapter.google.common;


/**
 * This class represents number of updated rows
 * 
 * @author felias
 * 
 */
public class UpdateResult {

	private int expectedNumberOfRows = -1;
	private int actualNumberOfRows = -1;

	/**
	 * 
	 * @param expectedNumberOfRows
	 *            number of rows that should have been updated
	 * @param actualNumberOfRows
	 *            actual number of updated rows
	 */
	public UpdateResult(int expectedNumberOfRows, int actualNumberOfRows) {
		this.expectedNumberOfRows = expectedNumberOfRows;
		this.actualNumberOfRows = actualNumberOfRows;
	}

	/*
	 * Returns number of rows that should have been updated
	 */
	public int getExpectedNumberOfRows() {
		return expectedNumberOfRows;
	}

	/**
	 * Returns actual number of updated rows
	 * 
	 */
	public int getActualNumberOfRows() {
		return actualNumberOfRows;
	}

}
