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

package org.teiid.dqp.message;

import java.util.List;

import org.teiid.translator.CacheDirective.Scope;


public class AtomicResultsMessage {

	private List<?>[] results;

    // Final row index in complete result set, if known
    private long finalRow = -1;
    
    // by default we support implicit close.
    private boolean supportsImplicitClose = true;
    
    private List<Exception> warnings;
    
    private Scope scope;

    // to honor the externalizable contract
	public AtomicResultsMessage() {
	}
	
	public AtomicResultsMessage(List<?>[] results) {
        this.results = results;
	}
	
    public boolean supportsImplicitClose() {
        return this.supportsImplicitClose;
    }
    
    public void setSupportsImplicitClose(boolean supportsImplicitClose) {
        this.supportsImplicitClose = supportsImplicitClose;
    }    
    
    public long getFinalRow() {
        return finalRow;
    }
    
    public void setFinalRow(long i) {
        finalRow = i;
    }

	public List[] getResults() {
		return results;
	}

	public void setWarnings(List<Exception> warnings) {
		this.warnings = warnings;
	}
	
	public List<Exception> getWarnings() {
		return warnings;
	}
	
	public void setScope(Scope scope) {
		this.scope = scope;
	}
	
	public Scope getScope() {
		return scope;
	}
}
