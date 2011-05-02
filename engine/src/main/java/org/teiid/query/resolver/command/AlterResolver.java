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

package org.teiid.query.resolver.command;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.resolver.CommandResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Alter;
import org.teiid.query.sql.lang.AlterProcedure;
import org.teiid.query.sql.lang.AlterTrigger;
import org.teiid.query.sql.lang.Command;

public class AlterResolver implements CommandResolver {

	@Override
	public void resolveCommand(Command command, TempMetadataAdapter metadata,
			boolean resolveNullLiterals) throws QueryMetadataException,
			QueryResolverException, TeiidComponentException {
		Alter<? extends Command> alter = (Alter<? extends Command>)command;
		ResolverUtil.resolveGroup(alter.getTarget(), metadata);
		int type = Command.TYPE_QUERY;
		boolean viewTarget = true;
		if (alter instanceof AlterTrigger) {
			TriggerEvent event = ((AlterTrigger)alter).getEvent();
			switch (event) {
			case DELETE:
				type = Command.TYPE_DELETE;
				break;
			case INSERT:
				type = Command.TYPE_INSERT;
				break;
			case UPDATE:
				type = Command.TYPE_UPDATE;
				break;
			}
		} else if (alter instanceof AlterProcedure) {
			type = Command.TYPE_STORED_PROCEDURE;
			viewTarget = false;
		}
		if (viewTarget && !QueryResolver.isView(alter.getTarget(), metadata)) {
			throw new QueryResolverException(QueryPlugin.Util.getString("AlterResolver.not_a_view", alter.getTarget())); //$NON-NLS-1$
		}
		if (alter.getDefinition() != null) {
			QueryResolver.resolveCommand(alter.getDefinition(), alter.getTarget(), type, metadata.getDesignTimeMetadata());
		}
	}

}
