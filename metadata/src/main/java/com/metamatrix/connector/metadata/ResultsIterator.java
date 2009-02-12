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

/**
 * 
 */
package com.metamatrix.connector.metadata;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class ResultsIterator implements Iterator {
	
	public interface ResultsProcessor {

		void createRows(Object resultObject, List rows);

	}
	
	private final ResultsProcessor objectQueryProcessor;
	private LinkedList rowBuffer = new LinkedList();
	private Iterator resultsIter;

	public ResultsIterator(ResultsProcessor objectQueryProcessor, Iterator resultsIter) {
		this.objectQueryProcessor = objectQueryProcessor;
		this.resultsIter = resultsIter;
	}

	public boolean hasNext() {
		return rowBuffer.size() > 0 || resultsIter.hasNext();
	}

	public Object next() {
		if (rowBuffer.size() > 0) {
			return rowBuffer.removeFirst();
		}
		if (!resultsIter.hasNext()) {
			throw new NoSuchElementException();
		}
		this.objectQueryProcessor.createRows(resultsIter.next(), rowBuffer);
		return rowBuffer.removeFirst();
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
	
}