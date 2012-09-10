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

package org.teiid.query.optimizer.relational;


public final class PlanHints { 

    // This flag indicates that the plan has a criteria somewhere
    public boolean hasCriteria = false;
    
    // This flag indicates that the plan has a join somewhere
    public boolean hasJoin = false;
    
    // This flag indicates that the plan has a virtual group somewhere
    public boolean hasVirtualGroups = false;
    
    // flag indicates that the plan has a union somewhere
    public boolean hasSetQuery = false;
    
    // flag indicating that the plan has a grouping node somewhere
    public boolean hasAggregates = false;
        
    public boolean hasLimit = false;
    
    public boolean hasOptionalJoin = false;
    
    public boolean hasRelationalProc = false;

	public boolean hasFunctionBasedColumns;
    
    public PlanHints() { }    
    
    public String toString(){
    	return "PlanHints"; //$NON-NLS-1$
    }
}
