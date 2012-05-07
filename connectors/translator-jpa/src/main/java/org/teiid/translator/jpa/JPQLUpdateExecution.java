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
package org.teiid.translator.jpa;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.teiid.core.TeiidException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.language.*;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public class JPQLUpdateExecution extends JPQLBaseExecution implements UpdateExecution {
	private Command command;
	private int[] results;
	
	public JPQLUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, EntityManager em) {
		super(executionContext, metadata, em);
		this.command = command;
	}
	
	@Override
	public void execute() throws TranslatorException {
		if (command instanceof BatchedUpdates) {
			BatchedUpdates updates = (BatchedUpdates)this.command;
			this.results = new int[updates.getUpdateCommands().size()];
			int index = 0;
			for (Command cmd:updates.getUpdateCommands()) {
				this.results[index++] = executeUpdate(cmd);
			}
		}
		else if (this.command instanceof Insert) {
			this.results = new int[1];
			Object entity = handleInsert((Insert)this.command);
			this.enityManager.merge(entity);
			this.results[0] = 1;
		}
		else {
			// update or delete
			this.results = new int[1];
			this.results[0] = executeUpdate(this.command);			
		}
	}

	private int executeUpdate(Command cmd) throws TranslatorException {
		String jpql = JPQLUpdateQueryVisitor.getJPQLString(cmd);
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "JPA Source-Query:", jpql); //$NON-NLS-1$
		Query query = this.enityManager.createQuery(jpql);
		return query.executeUpdate();
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		return this.results;
	}

	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}
	
	private Object handleInsert(Insert insert) throws TranslatorException {
		try {
			String entityClassName = insert.getTable().getMetadataObject().getProperty(JPAMetadataProcessor.ENTITYCLASS, false);
			Object entity = ReflectionHelper.create(entityClassName, null, this.executionContext.getCommandContext().getVDBClassLoader());
			
			List<ColumnReference> columns = insert.getColumns();
			List<Expression> values = ((ExpressionValueSource)insert.getValueSource()).getValues();
			if(columns.size() != values.size()) {
				throw new TranslatorException(JPAPlugin.Util.gs(JPAPlugin.Event.TEIID14007));
			}
			for(int i = 0; i < columns.size(); i++) {
				Column column = columns.get(i).getMetadataObject();
				Object value = values.get(i);
				
				// do not add the derived columns
				String name = column.getProperty(JPAMetadataProcessor.KEY_ASSOSIATED_WITH_FOREIGN_TABLE, false); 				
				if (name == null) {
					if(value instanceof Literal) {
						Literal literalValue = (Literal)value;
						PropertiesUtils.setBeanProperty(entity, column.getName(), literalValue.getValue());
					} else {
						PropertiesUtils.setBeanProperty(entity, column.getName(), value);
					}
				}
			}			
			return entity;
		} catch (TeiidException e) {
			throw new TranslatorException(e);
		}
	}
}
