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

package org.teiid.translator.infinispan.dsl;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.teiid.core.TeiidException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.query.eval.TeiidScriptEngine;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.infinispan.dsl.metadata.ProtobufMetadataProcessor;

/**
 */
public class InfinispanUpdateExecution implements UpdateExecution {


	private ScriptContext sc = new SimpleScriptContext();

	// Passed to constructor
	private InfinispanConnection connection;
	private ExecutionContext context;
	private InfinispanExecutionFactory executionFactory;
	private Command command;

	private  TeiidScriptEngine scriptEngine;

	protected int fetchSize;
	private int updateCnt = 0;

	public InfinispanUpdateExecution(Command command,
			InfinispanConnection connection, ExecutionContext context,
			InfinispanExecutionFactory env) {
		this.connection = connection;
		this.context = context;
		fetchSize = context.getBatchSize();
		this.command = command;
		this.executionFactory = env;
		scriptEngine = this.connection.getClassRegistry().getReadScriptEngine();

	}

	// ===========================================================================================================================
	// Methods
	// ===========================================================================================================================

	@Override
	public void execute() throws TranslatorException {
//		if (command instanceof BatchedUpdates) {
//			BatchedUpdates updates = (BatchedUpdates)this.command;
//			this.results = new int[updates.getUpdateCommands().size()];
//			int index = 0;
//			for (Command cmd:updates.getUpdateCommands()) {
//				this.results[index++] = executeUpdate(cmd);
//			}
//		}


		if (command instanceof Update) {
			handleUpdate((Update) command);
		} else if (command instanceof Delete) {
			handleDelete((Delete) command);
		} else if (command instanceof Insert) {
			handleInsert((Insert) command);
		} else {
			final String msg = InfinispanPlugin.Util
					.getString("InfinispanUpdateExecution.incorrectCommandError"); //$NON-NLS-1$
			throw new TranslatorException(msg);
		}
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException,
			TranslatorException {
		return new int[] { updateCnt };
	}

	@SuppressWarnings("null")
	private void handleInsert(Insert insert) throws TranslatorException {
		try {

			// get the className defined as the entity_class on the table being
			// updated
			String className = insert.getTable().getMetadataObject()
					.getProperty(ProtobufMetadataProcessor.ENTITYCLASS, false);
			if (className == null) {
				throw new TranslatorException(
						ProtobufMetadataProcessor.ENTITYCLASS
								+ " property not found defined for table "
								+ insert.getTable().getName());
			}

			// get the class to instantiate instance
			Class<?> clz = this.connection.getClassRegistry()
					.getRegisteredClass(className);
			if (clz == null) {
				throw new TranslatorException("Registered Class not found for "
						+ className);
			}

			Object entity = null;
			try {
				// create the new instance from the classLoader the the class
				// was created from,
				// which is were the clz came from
				entity = clz.newInstance();
			} catch (InstantiationException e) {
			} catch (IllegalAccessException e) {
			}

			String cacheName = insert.getTable().getMetadataObject()
					.getNameInSource();

			ForeignKey fk = getForeignKeyColumn(insert.getTable());
			String fkeyColNIS = null;
			if (fk != null) {
				fkeyColNIS = getForeignKeyNIS(insert.getTable(), fk);
			}
			Column keyCol = null;
			if (fkeyColNIS == null) {
				keyCol = insert.getTable().getMetadataObject().getPrimaryKey()
						.getColumns().get(0);
			} 
			
			if (fkeyColNIS == null && keyCol == null) {
				final String msg = InfinispanPlugin.Util
						.getString(
								"InfinispanUpdateExecution.noPrimaryOrForeignKeyForTable", insert.getTable().getName()); //$NON-NLS-1$
				throw new TranslatorException(msg);
			}

			List<ColumnReference> columns = insert.getColumns();
			List<Expression> values = ((ExpressionValueSource) insert
					.getValueSource()).getValues();
			if (columns.size() != values.size()) {
				throw new TranslatorException(
						"Column and Value Size's don't match");
			}

			Object keyValue = null;
			Object fkeyValue = null;

			for (int i = 0; i < columns.size(); i++) {
				Column column = columns.get(i).getMetadataObject();
				Object value = values.get(i);

				// do not add the foreign key columns
				if (fkeyColNIS != null && fkeyColNIS.equals(column.getName()) ) {
					if (value instanceof Literal) {
						Literal literalValue = (Literal) value;
						fkeyValue = literalValue.getValue();
					} else {
						fkeyValue = value;
					}
					
				} else if ( keyCol != null && keyCol.getName().equals(column.getName()) ) {
					if (value instanceof Literal) {
						Literal literalValue = (Literal) value;
						keyValue = literalValue.getValue();
					} else {
						keyValue = value;
					}

				} else {
					
					if (value instanceof Literal) {
						Literal literalValue = (Literal) value;
						PropertiesUtils.setBeanProperty(entity,
								column.getName(), literalValue.getValue());
					} else {
						PropertiesUtils.setBeanProperty(entity,
								column.getName(), value);
					}	
				}
			}
			
			Map<Object, Object> cache = connection.getCache(cacheName);
			if (keyCol != null) {
				cache.put(keyValue, entity);
			} else {
				Object rootObject = this.executionFactory.performKeySearch(cacheName, fkeyColNIS, fkeyValue, connection, context);
				String fk_nis = fk.getNameInSource();
				
				Object childrenObjects = this.evaluate(rootObject, fk_nis);
					
				if (Collection.class.isAssignableFrom(childrenObjects.getClass())) {
					Collection c = (Collection) childrenObjects;
					c.add(entity);

					PropertiesUtils.setBeanProperty(rootObject, fk_nis, c);

				} else if (Map.class.isAssignableFrom(childrenObjects.getClass())) {
					final String msg = InfinispanPlugin.Util
							.getString(
									"InfinispanUpdateExecution.mapsNotSupported"); //$NON-NLS-1$
					throw new TranslatorException(msg);						

				} else if (childrenObjects.getClass().isArray()) {
					Object[] a = (Object[]) childrenObjects;
					Object[] n = new Object[a.length + 1];
					int i = 0;
					for (Object o:a) {
						n[i] = a[i++];
					}
					n[i] = entity;
					PropertiesUtils.setBeanProperty(rootObject, fk_nis, n);
				} 
				
				cache.put(fkeyValue, rootObject);

			}
			++updateCnt;
			
		} catch (TeiidException e) {
			throw new TranslatorException(e);
		}
	}

	// Private method to actually do a delete operation. 
	private void handleDelete(Delete delete) throws TranslatorException {

		Column keyCol = null;
		String fkeyColNIS = null;
		
		// if fk exist, its assumed its a container class
		// don't want to use PK, cause it can exist on any table
		ForeignKey fk = getForeignKeyColumn(delete.getTable());
		if (fk == null) {
			keyCol = delete.getTable().getMetadataObject().getPrimaryKey().getColumns().get(0);

		} else {
			fkeyColNIS = getForeignKeyNIS(delete.getTable(), fk);			
		}		
			
		String cacheName = delete.getTable().getMetadataObject().getNameInSource();

		if (fkeyColNIS == null && keyCol == null) {
			final String msg = InfinispanPlugin.Util
					.getString(
							"InfinispanUpdateExecution.noPrimaryOrForeignKeyForTable", delete.getTable().getName()); //$NON-NLS-1$
			throw new TranslatorException(msg);
		}		

		Map<Object, Object> cache = connection.getCache(cacheName);

		// Find all the objects that meet the criteria for deletion
		List<Object> toDelete = this.executionFactory.search(delete, cacheName,
				connection, this.context);

		CompiledScript cs;
		if (keyCol != null) {
			// if this is the root class (no foreign key), then for each object, obtain
			// the primary key value and use it to be removed from the cache
			
			try {
				cs = scriptEngine.compile(ClassRegistry.OBJECT_NAME + "." + keyCol.getName());
	
				for (Object o : toDelete) {
					sc.setAttribute(ClassRegistry.OBJECT_NAME, o,
							ScriptContext.ENGINE_SCOPE);
					final Object v = cs.eval(sc);
					cache.remove(v);
					++updateCnt;
				}
	
			} catch (ScriptException e1) {
				final String msg = InfinispanPlugin.Util
						.getString("InfinispanUpdateExecution.scriptExecutionFailure"); //$NON-NLS-1$
				throw new TranslatorException(e1, msg);
			}
		} 
		
		// TODO:  delete container class 
//		else {
//
//			try {
//				cs = scriptEngine.compile(ClassRegistry.OBJECT_NAME + "." + fkeyColNIS);
//	
//				for (Object o : toDelete) {
//					sc.setAttribute(ClassRegistry.OBJECT_NAME, o,
//							ScriptContext.ENGINE_SCOPE);
//					// get root key value
//					Object rootKeyValue = cs.eval(sc);
//					Object rootObject = this.executionFactory.performKeySearch(cacheName, fkeyColNIS, rootKeyValue, connection, context);
//
//					String fk_nis = fk.getNameInSource();
//					Object childrenObjects = this.evaluate(rootObject, fk_nis);
//					
//					if (Collection.class.isAssignableFrom(childrenObjects.getClass())) {
//						Collection c = (Collection) childrenObjects;
//						c.remove(o);
//
//						PropertiesUtils.setBeanProperty(rootObject, fk_nis, c);
//
//					} else if (Map.class.isAssignableFrom(childrenObjects.getClass())) {
//						final String msg = InfinispanPlugin.Util
//								.getString(
//										"InfinispanUpdateExecution.mapsNotSupported"); //$NON-NLS-1$
//						throw new TranslatorException(msg);						
//
//					} else if (childrenObjects.getClass().isArray()) {
//						Object[] a = (Object[]) childrenObjects;
//						int foundMatch=-1;
//						// first find the object match location
//						for (int i=0; i<a.length; i++) {
//							if (a[i].equals(o)) {
//								foundMatch = i;
//								break;
//							}
//						}
//						// shrink array by 1, by not copying the matched objec
//						if (foundMatch >= 0) {
//							Object[] n = new Object[a.length - 1];
//							int loc=0;
//							for (int i=0; i<a.length; i++) {
//								if (i == foundMatch) continue;
//								n[loc] = a[i];
//								++loc;
//							}
//							// set the array on the root object
//							PropertiesUtils.setBeanProperty(rootObject, fk_nis, n);
//						}
//					} 
//					
//					cache.put(rootKeyValue, rootObject);
//					
//
////					cache.remove(v);
//					++updateCnt;
//				}
//	
//			} catch (ScriptException e1) {
//				final String msg = InfinispanPlugin.Util
//						.getString("InfinispanUpdateExecution.scriptExecutionFailure"); //$NON-NLS-1$
//				throw new TranslatorException(e1, msg);
//			}

	}

	// Private method to actually do an update operation. 
	private void handleUpdate(Update update) throws TranslatorException {

		String cacheName = update.getTable().getMetadataObject().getNameInSource();

		// Find all the objects that meet the criteria for updating
		List<Object> toUpdate = this.executionFactory.search(update, cacheName,
				connection, this.context);
		
		if (toUpdate == null || toUpdate.size() == 0){
			return;
		}
		
		Column keyCol = null;
		String fkeyColNIS = null;
		
		// if fk exist, its assumed its a container class
		// don't want to use PK, cause it can exist on any table
		ForeignKey fk = getForeignKeyColumn(update.getTable());
		if (fk == null) {
			keyCol = update.getTable().getMetadataObject().getPrimaryKey().getColumns().get(0);

		} else {
			fkeyColNIS = getForeignKeyNIS(update.getTable(), fk);			
		}		
		
		if (fkeyColNIS == null && keyCol == null) {
			final String msg = InfinispanPlugin.Util
					.getString(
							"InfinispanUpdateExecution.noPrimaryOrForeignKeyForTable", update.getTable().getName()); //$NON-NLS-1$
			throw new TranslatorException(msg);
		}
	
		Map<Object, Object> cache = connection.getCache(cacheName);	
		
		Object keyValue = null;
		Object fkeyValue = null;

		List<SetClause> updateList = update.getChanges();
		
		if (keyCol != null) {
			for (Object entity:toUpdate) {
				
				for (SetClause sc:updateList) {
					Column column = sc.getSymbol().getMetadataObject();
					Object value = sc.getValue();
					
					if ( keyCol.getName().equals(column.getName()) ) {
						if (value instanceof Literal) {
							Literal literalValue = (Literal) value;
							keyValue = literalValue.getValue();
						} else {
							keyValue = value;
						}
		
					} else {
						
						if (value instanceof Literal) {
							Literal literalValue = (Literal) value;
							PropertiesUtils.setBeanProperty(entity,
									column.getName(), literalValue.getValue());
						} else {
							PropertiesUtils.setBeanProperty(entity,
									column.getName(), value);
						}	
					}			
				}
				
				cache.put(keyValue, entity);
				++updateCnt;
			
			}
			
		} else {
			
		}

		++updateCnt;
	}

	public void cancel() throws TranslatorException {
		close();
	}

	public void close() {
		this.connection = null;
		this.command = null;
		this.context = null;
		this.executionFactory = null;
		sc = null;
	}

	private ForeignKey getForeignKeyColumn(NamedTable table) {
		ForeignKey fk = null;
		// there should only be 1 foreign key that relates to its parent
		List<ForeignKey> fkeys = table.getMetadataObject().getForeignKeys();
		if (fkeys.size() > 0) {
			fk = fkeys.get(0);
			return fk;
		}
		return fk;
	}
	
	private String getForeignKeyNIS(NamedTable table, ForeignKey fk) throws TranslatorException {

		String fkeyColNIS = null;
		
		if (fk != null) {
			if (fk.getReferenceKey() != null) {
				Column fkeyCol = fk.getReferenceKey().getColumns().get(0);
				fkeyColNIS = fkeyCol.getNameInSource();
			} else if (fk.getReferenceColumns() != null) {
				fkeyColNIS = fk.getReferenceColumns().get(0);
			}
		}
		
		return fkeyColNIS;

	}

	private Object evaluate(Object value, String columnName)
			throws TranslatorException {
		sc.setAttribute(ClassRegistry.OBJECT_NAME, value,
				ScriptContext.ENGINE_SCOPE);
		Object v;
		try {
			CompiledScript cs = scriptEngine.compile(ClassRegistry.OBJECT_NAME
					+ "." + columnName);
			v = cs.eval(sc);
		} catch (ScriptException e) {
			throw new TranslatorException(e);
		}
		return v;

	}
}
