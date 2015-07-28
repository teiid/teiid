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

import static org.teiid.language.visitor.SQLStringVisitor.*;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.infinispan.client.hotrod.RemoteCache;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.query.eval.TeiidScriptEngine;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

/**
 */
public class InfinispanUpdateExecution implements UpdateExecution {
	public static final String GET = "get"; //$NON-NLS-1$
	public static final String SET = "set"; //$NON-NLS-1$
	public static final String IS = "is"; //$NON-NLS-1$

	private ScriptContext sc = new SimpleScriptContext();

	// Passed to constructor
	private InfinispanConnection connection;
	private ExecutionContext context;
	private InfinispanExecutionFactory executionFactory;
	private Command command;

	private  TeiidScriptEngine scriptEngine;
	private ClassRegistry classRegistry;

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
		classRegistry = this.connection.getClassRegistry();
		scriptEngine = classRegistry.getReadScriptEngine();

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
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25002, new Object[] {command.getClass().getName()}));
		}
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException,
			TranslatorException {
		return new int[] { updateCnt };
	}

	@SuppressWarnings({ "unchecked" })
	private void handleInsert(Insert insert) throws TranslatorException {

		// get the class to instantiate instance
		Class<?> clz = this.classRegistry.getRegisteredClassUsingTableName(insert.getTable().getName());
		if (clz == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25003, new Object[] {insert.getTable().getName()}));
		}
		
		Map<String, Method> writeMethods = this.classRegistry.getWriteClassMethods(clz.getSimpleName());

		Object entity = null;
		try {
			// create the new instance from the classLoader the the class
			// was created from
			entity = clz.newInstance();
		} catch (Exception e) {
			throw new TranslatorException(e);
		} 

		String cacheName = insert.getTable().getMetadataObject()
				.getNameInSource();

		// first determine if the table to be inserted into has a foreign key relationship, which
		// would then be considered a child table, otherwise it must have a primary key
		ForeignKey fk = getForeignKeyColumn(insert.getTable());
		String fkeyColNIS = null;
		if (fk != null) {
			fkeyColNIS = getForeignKeyNIS(insert.getTable(), fk);
		}
		Column keyCol = null;
		if (fkeyColNIS == null) {
			keyCol = getPrimaryKeyColumn(insert.getTable());
		} 
		
		if (fkeyColNIS == null && keyCol == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25004, new Object[] {"insert", insert.getTable().getName()}));
		}

		List<ColumnReference> columns = insert.getColumns();
		List<Expression> values = ((ExpressionValueSource) insert
				.getValueSource()).getValues();
		if (columns.size() != values.size()) {
			throw new TranslatorException(
					"Program error, Column Metadata Size [" + columns.size() + "] and Value Size [" + values.size() + "] don't match ");
		}

		Object keyValue = null;
		Object fkeyValue = null;
		

		for (int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i).getMetadataObject();
			Object value = values.get(i);

			if ( keyCol != null && keyCol.getName().equals(column.getName()) ) {

				if (value instanceof Literal) {
					Literal literalValue = (Literal) value;
					value = literalValue.getValue();
				}
					
				writeColumnData(entity, column, value, writeMethods);
				
				keyValue = evaluate(entity, getRecordName(keyCol));
				
			} else 	if (fkeyColNIS != null && fkeyColNIS.equals(column.getName()) ) {
				if (value instanceof Literal) {
					Literal literalValue = (Literal) value;
					fkeyValue = literalValue.getValue();
				} else {
					fkeyValue = value;
				}

			} else {
				
				if (value instanceof Literal) {
					Literal literalValue = (Literal) value;
					value = literalValue.getValue();
				}
				
				writeColumnData(entity, column, value, writeMethods);

			}
		}
		
		RemoteCache<Object, Object> cache = (RemoteCache<Object, Object>) connection.getCache();
		keyValue = convertKeyValue(keyValue);
		if (keyCol != null) {
			Object rootObject = this.executionFactory.performKeySearch(cacheName, getRecordName(keyCol), keyValue, connection, context);

			if (rootObject != null) {
				throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25009, new Object[] {insert.getTable().getName(), keyValue}));
			}
			cache.put(keyValue, entity);
		} else {
			fkeyValue = convertKeyValue(fkeyValue);
			Object rootObject = this.executionFactory.performKeySearch(cacheName, fkeyColNIS, fkeyValue, connection, context);
			String fk_nis = fk.getNameInSource();
			
			Object childrenObjects = this.evaluate(rootObject, fk_nis);
				
			if (Collection.class.isAssignableFrom(childrenObjects.getClass())) {
				@SuppressWarnings("rawtypes")
				Collection c = (Collection) childrenObjects;
				c.add(entity);

				PropertiesUtils.setBeanProperty(rootObject, fk_nis, c);

			} else if (Map.class.isAssignableFrom(childrenObjects.getClass())) {
				throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25005, new Object[] {insert.getTable().getName()}));

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
			
			cache.replace(fkeyValue, rootObject);

		}
		++updateCnt;

	}

	// Private method to actually do a delete operation. 
	private void handleDelete(Delete delete) throws TranslatorException {

		Column keyCol = null;
		String fkeyColNIS = null;
		
		// if fk exist, its assumed its a container class
		// don't want to use PK, cause it can exist on any table
		ForeignKey fk = getForeignKeyColumn(delete.getTable());
		if (fk == null) {
			keyCol = getPrimaryKeyColumn(delete.getTable());

		} else {
			fkeyColNIS = getForeignKeyNIS(delete.getTable(), fk);			
		}		
			
		String cacheName = delete.getTable().getMetadataObject().getSourceName();

		if (fkeyColNIS == null && keyCol == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25004, new Object[] {"delete",delete.getTable().getName()}));
		}		

		// Find all the objects that meet the criteria for deletion
		List<Object> toDelete = this.executionFactory.search(delete, cacheName,
				connection, this.context);

		CompiledScript cs;
		if (keyCol != null) {
			// if this is the root class (no foreign key), then for each object, obtain
			// the primary key value and use it to be removed from the cache
			@SuppressWarnings("rawtypes")
			RemoteCache cache = (RemoteCache) connection.getCache();
			
			try {
								
				cs = scriptEngine.compile(ClassRegistry.OBJECT_NAME + "." +  getRecordName(keyCol));
	
				for (Object o : toDelete) {
					sc.setAttribute(ClassRegistry.OBJECT_NAME, o,
							ScriptContext.ENGINE_SCOPE);
					Object v = cs.eval(sc);

					cache.remove(convertKeyValue(v));
					if (cache.containsKey(v)) {
						throw new TranslatorException("Program error, object for key: " + v.toString() + " was not removed");
					}
					++updateCnt;
				}
	
			} catch (ScriptException e1) {
				throw new TranslatorException(e1);
			}
		} else {
			throw new TranslatorException("Deleting container class is not currently supported");

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
		
		// get the class to instantiate instance
		Class<?> clz = this.classRegistry.getRegisteredClassUsingTableName(update.getTable().getName());
		if (clz == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25003, new Object[] {update.getTable().getName()}));
		}
		
		Map<String, Method> writeMethods = this.classRegistry.getWriteClassMethods(clz.getSimpleName());
		

		String cacheName = update.getTable().getMetadataObject().getSourceName();

		// Find all the objects that meet the criteria for updating
		List<Object> toUpdate = this.executionFactory.search(update, cacheName,
				connection, this.context);
		
		if (toUpdate == null || toUpdate.size() == 0){
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
					"InfinispanUpdateExecution.update: no objects found to update based on - " + update.toString()); //$NON-NLS-1$
			return;
		}
		
		Column keyCol = null;
		String fkeyColNIS = null;
		
		// if fk exist, its assumed its a container class
		// don't want to use PK, cause it can exist on any table
		ForeignKey fk = getForeignKeyColumn(update.getTable());
		if (fk == null) {
			keyCol = getPrimaryKeyColumn(update.getTable());

		} else {
			fkeyColNIS = getForeignKeyNIS(update.getTable(), fk);			
		}		
		
		if (fkeyColNIS == null && keyCol == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25004, new Object[] {"update",update.getTable().getName()}));
		}
	
		@SuppressWarnings({ "rawtypes", "unchecked" })
		RemoteCache<Object, Object> cache = (RemoteCache) connection.getCache();
		
		Object keyValue = null;

		List<SetClause> updateList = update.getChanges();
		
		if (keyCol != null) {
			for (Object entity:toUpdate) {
				
				keyValue = evaluate(entity, getRecordName(keyCol));
				
				for (SetClause sc:updateList) {
					Column column = sc.getSymbol().getMetadataObject();
					Object value = sc.getValue();
					
					if ( keyCol.getName().equals(column.getName()) ) {
						throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25006, new Object[] {keyCol.getName(),update.getTable().getName()}));						
					}
					
					if (value instanceof Literal) {
						Literal literalValue = (Literal) value;
						value = literalValue.getValue();

					} 
					
					writeColumnData(entity, column, value, writeMethods);
			
				}
				
				cache.replace( convertKeyValue(keyValue), entity);
				++updateCnt;
			
			}
			
		} 
			//logic for updating container classes
	}

	@Override
	public void cancel() throws TranslatorException {
		close();
	}

	@Override
	public void close() {
		this.connection = null;
		this.command = null;
		this.context = null;
		this.executionFactory = null;
		this.classRegistry = null;
		this.scriptEngine = null;
		sc = null;
	}
	
	private Column getPrimaryKeyColumn(NamedTable table) {
		if (table.getMetadataObject().getPrimaryKey() != null) {
			return table.getMetadataObject().getPrimaryKey()
					.getColumns().get(0);					
		}
		return null;
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
				fkeyColNIS = fkeyCol.getSourceName();
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
		try {
			CompiledScript cs = scriptEngine.compile(ClassRegistry.OBJECT_NAME
					+ "." + columnName);
			final Object v = cs.eval(sc);
			return v;
		} catch (ScriptException e) {
			throw new TranslatorException(e);
		}

	}
	
	private  Object convertKeyValue(Object value )  throws TranslatorException {
	
		try {
			if (connection.getCacheKeyClassType() == null) {
				return value;
			}
			
			return DataTypeManager.transformValue(value,  connection.getCacheKeyClassType());
		} catch (TransformationException e) {
			throw new TranslatorException(e);
		}

	}
	
	private void writeColumnData(Object entity, Column column, Object value, Map<String, Method> writeMethods) throws TranslatorException {
		
			String nis = getRecordName(column);
			
			// if this is a child table, the nis will have the table.columnName formatted
			if (nis.contains(".")) {
				nis = StringUtil.getLastToken(nis, ".");
			}

			Method m = writeMethods.get(nis);
			if (m == null) {
				throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25061, new Object[] {nis}));

			}
			try {
				if (value == null) {
					ClassRegistry.executeSetMethod(m, entity, value);
					return;
				}
				final Class<?> sourceClassType = value.getClass();
				final Class<?> targetClassType = m.getParameterTypes()[0];
				if (targetClassType.isEnum()) {
					Object[] con = targetClassType.getEnumConstants();
					for (Object c:con) {
						if (c.toString().equalsIgnoreCase(value.toString())) {
							ClassRegistry.executeSetMethod(m, entity, c);
							return;
						}
					}
					throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25061, new Object[] {nis}));
				} else if (sourceClassType.isAssignableFrom(targetClassType)) {
					ClassRegistry.executeSetMethod(m, entity, value);
				} else if (DataTypeManager.isTransformable(value.getClass(), targetClassType)) {
	
					final Object transformedValue = DataTypeManager.transformValue(value,  targetClassType ); //column.getJavaType()
					ClassRegistry.executeSetMethod(m, entity, transformedValue);
				
//				} else if (value.getClass().isArray()) {
//						final ByteString bs = ByteString.copyFrom( (byte[]) value);
//						ClassRegistry.executeSetMethod(m, entity, bs);
//					} else {
//						final Object transformedValue = DataTypeManager.transformValue(value,  column.getJavaType());
//						ClassRegistry.executeSetMethod(m, entity, transformedValue);
//					}
								
				} else {
					PropertiesUtils.setBeanProperty(entity, nis, value);
				}

				
			} catch (Exception e) {
				throw new TranslatorException(e);
			}

	}
}
