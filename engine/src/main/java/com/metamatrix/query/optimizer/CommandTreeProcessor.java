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

package com.metamatrix.query.optimizer;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.metadata.QueryMetadataInterface;

/**
 * An object that performs some processing on a tree of 
 * {@link CommandTreeNode CommandTreeNodes} during command
 * planning.
 */
public interface CommandTreeProcessor {
	
	/**
	 * Do any necessary processing on the given tree of
	 * command tree nodes; return the root of the modified
	 * tree.
	 * @param root root of the tree of nodes
	 * @return root of the modified tree, or just the root
	 * param if no modification occurred.
	 * @param metadata source of metadata 
	 */
	CommandTreeNode process(CommandTreeNode root, QueryMetadataInterface metadata)
	throws QueryMetadataException, MetaMatrixComponentException; 

}
