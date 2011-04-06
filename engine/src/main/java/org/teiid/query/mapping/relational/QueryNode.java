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

package org.teiid.query.mapping.relational;

import java.util.ArrayList;
import java.util.List;

import org.teiid.query.sql.lang.Command;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;


/**
 * <p>The QueryNode represents a virtual or temporary group in the modeler.  QueryNodes may
 * be nested to indicate data queries built from other virtual or temporary groups.  The
 * root node of a tree of QueryNode objects should be defining a virtual group.  Leaves
 * should be other physical or virtual groups.  Internal nodes of the tree are temporary
 * groups.</p>
 *
 * <p>A QueryNode must have a group name and a query.  It may have a command (just used
 * for convenient storage during conversion - this is not persisted).</p>
 */
public class QueryNode {

	// Initial state
	private String query;
	private List<String> bindings;     // optional - construct if needed
	private String user;
	// After parsing and resolution
	private Command command;
	private UpdateInfo updateInfo;

    /**
     * Construct a query node with the required parameters.
     * @param query SQL query
     */
	public QueryNode(String query) {
		this.query = query;
	}

    /**
     * Get SQL query
     * @return SQL query
     */
	public String getQuery() {
		return this.query;
	}

    /**
     * Set the SQL query
     * @param String query
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * Add parameter binding to this node.  Bindings should be added in
     * the order they appear in the query.
     * @param binding Binding reference
     */
    public void addBinding(String binding) {
        if(this.bindings == null) {
            this.bindings = new ArrayList<String>();
        }
        this.bindings.add(binding);
    }

    /**
     * Get list of bindings.
     * @return bindings
     */
    public List<String> getBindings() {
        return this.bindings;
    }

    /**
     * Set all of the bindings (existing are dropped)
     * @param bindings New bindings
     */
    public void setBindings(List<String> bindings) {
        this.bindings = new ArrayList<String>(bindings);
    }

    /**
     * Set command - this is provided as a convenient place to cache this command
     * during conversion.
     * @param command Command corresponding to query
     */
    public void setCommand(Command command) {
        this.command = command;
    }

    /**
     * Get command corresponding to query, may be null
     * @return command Command corresponding to query
     */
    public Command getCommand() {
        return this.command;
    }

	/**
	 * Print plantree structure starting at this node
	 * @return String representing this node and all children under this node
	 */
	public String toString() {
        return query;
	}
	
	public String getUser() {
		return user;
	}
	
	public void setUser(String user) {
		this.user = user;
	}
	
	public UpdateInfo getUpdateInfo() {
		return updateInfo;
	}
	
	public void setUpdateInfo(UpdateInfo updateInfo) {
		this.updateInfo = updateInfo;
	}
	
}
