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

package org.teiid.query.processor;

import java.util.ArrayList;
import java.util.List;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;


public class FakeTupleSource implements TupleSource {
	
	static int maxOpen;
	static int open;
	
	static void resetStats() {
		maxOpen = 0;
		open = 0;
	}
    
    public static class FakeComponentException extends TeiidComponentException {
        
    }

	private List elements;
	private List[] tuples;
	private int index = 0;
	private List expectedSymbols;
	private int[] columnMap;
    
    //used to test blocked exception. If true,
    //the first time nextTuple is called, it will throws BlockedExceptiom
    private boolean blockOnce;
    
	public FakeTupleSource(List elements, List[] tuples) {
		this.elements = elements;
		this.tuples = tuples; 
	}

	public FakeTupleSource(List elements, List[] tuples, List expectedSymbols, int[] columnMap) {
		this.elements = elements;
		this.tuples = tuples; 
		this.expectedSymbols = expectedSymbols;
		this.columnMap = columnMap;
		open++;
		maxOpen = Math.max(open, maxOpen);
	}

	public List getSchema() { 
        List theElements = null;        
	    if(expectedSymbols != null) {
			theElements = expectedSymbols;
	    } else {
	    	theElements = elements;    
	    }
        
        return theElements;
	}
	
	public void openSource() {				
		index = 0;
	}

	public List nextTuple()
		throws TeiidComponentException {
	
        if(this.blockOnce){
            this.blockOnce = false;
            throw BlockedException.INSTANCE;            
        }
        
		if(index < tuples.length) { 
		    // Get full data tuple, with elements
		    List tuple = tuples[index++];
		    
		    if(expectedSymbols == null) { 
		        return tuple;
		    }
		    // Build mapped data tuple, with expectedSymbols
		    List mappedTuple = new ArrayList(expectedSymbols.size());
		    for(int i=0; i<columnMap.length; i++) { 
		    	int colIndex = columnMap[i];
                if(colIndex >= 0) {
                    mappedTuple.add( tuple.get(colIndex) );
                } else {
                    mappedTuple.add( null );
                }
		    }		    
			return mappedTuple;
		}
		return null;
	}

	public void closeSource() {
		open--;
	}
    
    public void setBlockOnce(){
        this.blockOnce = true;
    }

}
