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
 
package org.teiid.dqp.internal.process;

import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.CommandContext;

/**
 * Defines a validator that checks for proper authorization.  
 */
public interface AuthorizationValidator {
	
	enum CommandType {
		USER,
		PREPARED,
		CACHED
	}
	
	/**
	 * Validates the given command.  If the command is not a {@link CommandType#USER} command, the command object should not be modified.
	 * Any modification must be fully resolved using the associated {@link QueryMetadataInterface}.  Returning true for a 
	 *  {@link CommandType#PREPARED} or  {@link CommandType#CACHED} commands means that the matching prepared plan or cache entry
	 *  will not be used.
	 * @param originalSql array of commands will typically contain only a single string, but may have multiple for batched updates.
	 * @param command the parsed and resolved command. 
	 * @param metadata
	 * @param commandContext
	 * @param commandType
	 * @return true if the USER command was modified, or if the non-USER command should be modified.
	 * @throws QueryValidatorException
	 * @throws TeiidComponentException
	 */
	boolean validate(String[] originalSql, Command command, QueryMetadataInterface metadata, CommandContext commandContext, CommandType commandType) throws QueryValidatorException, TeiidComponentException;
	
	/**
	 * 
	 * @param roleName
	 * @param commandContext
	 * @return true if the current user has the given role
	 */
	boolean hasRole(String roleName, CommandContext commandContext);
	
	boolean isEnabled();
	
	void setEnabled(boolean enabled);
}
