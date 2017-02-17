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

package org.teiid.translator.object;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.object.util.ObjectUtil;

/**
 */
public class ObjectUpdateExecution extends ObjectBaseExecution implements UpdateExecution {

	private ScriptContext sc = new SimpleScriptContext();

	// Passed to constructor
	private Command command;
	private Class<?> clz =  null;

	private int[] result;

	public ObjectUpdateExecution(Command command,
			ObjectConnection connection, ExecutionContext context,
			ObjectExecutionFactory env) {
		super(connection, context, env);
		this.command = command;
	}

	// ===========================================================================================================================
	// Methods
	// ===========================================================================================================================

	@Override
	public void execute() throws TranslatorException {		
		int index = 0;

		if (command instanceof BatchedUpdates) {
			BatchedUpdates updates = (BatchedUpdates)this.command;
			result = new int[updates.getUpdateCommands().size()];
			for (Command cmd:updates.getUpdateCommands()) {
				
				this.result[index++] = executeUpdate(cmd);
			}		
			
		} else {
			result = new int[1];
			this.result[index] = executeUpdate(command);
		}
	
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException {
		return this.result;
	}
	
	private int executeUpdate(Command command ) throws TranslatorException {
		
		ClassRegistry classRegistry = this.getClassRegistry();
		ObjectVisitor visitor = createVisitor();
		visitor.visitNode(command);

		// throw the 1st exeception in the list
		if (visitor.getExceptions() != null && !visitor.getExceptions().isEmpty()) {
			throw visitor.getExceptions().get(0);
		}
		
		clz = ObjectUtil.getRegisteredClass(classRegistry, visitor);

		Map<String, Method> writeMethods = classRegistry.getWriteClassMethods(clz.getSimpleName());
		
		int cnt = 0;
		try {
			if (command instanceof Update) {
				if (visitor.getPrimaryTable() != null) {
					this.connection.getDDLHandler().setStagingTarget(true);
				}
				cnt = handleUpdate((Update) command, visitor, classRegistry, clz, writeMethods);
				
			} else if (command instanceof Delete) {
				if (visitor.getPrimaryTable() != null) {
					this.connection.getDDLHandler().setStagingTarget(true);
				}
				cnt = handleDelete((Delete) command, visitor, clz);
				
			} else if (command instanceof Insert) {
				if (visitor.getPrimaryTable() != null) {
					this.connection.getDDLHandler().setStagingTarget(true);
				}
				cnt = handleInsert((Insert) command, visitor, clz, writeMethods);
				
			} else {
				throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21004, new Object[] {command.getClass().getName()}));
			}
		} finally {
			this.connection.getDDLHandler().setStagingTarget(false);
		}
		return cnt;
	}

	private int handleInsert(Insert insert, ObjectVisitor visitor,  Class<?> clz, Map<String, Method> writeMethods) throws TranslatorException {

		Object entity = null;
		try {
			// create the new instance from the classLoader the the class
			// was created from
			entity = clz.newInstance();
		} catch (Exception e) {
			throw new TranslatorException(e);
		}  

		// first determine if the table to be inserted into has a foreign key relationship, which
		// would then be considered a child table, otherwise it must have a primary key
		ForeignKey fk = visitor.getForeignKey();
		
		if (fk != null) {
			int r = handleInsertChildObject(visitor, entity, writeMethods, clz);
			return r;
		}
		
		Column keyCol = null;
		keyCol = visitor.getPrimaryKeyCol();
		
		if (keyCol == null) {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21006, new Object[] {"insert", visitor.getTableName()}));
		}

		List<ColumnReference> columns = visitor.getInsert().getColumns();
		List<Expression> values = ((ExpressionValueSource) visitor.getInsert()
				.getValueSource()).getValues();
		Object keyValue = null;
		
		for (int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i).getMetadataObject();
			Object value = values.get(i);

			if (keyCol.getName().equals(column.getName()) ) {

				if (value instanceof Literal) {
					Literal literalValue = (Literal) value;
					value = literalValue.getValue();
				}
					
				writeColumnData(entity, column, value, writeMethods);
				
				keyValue = evaluate(entity, ObjectUtil.getRecordName(keyCol), connection.getClassRegistry().getReadScriptEngine(), clz);
				
			} else {
				
				if (value instanceof Literal) {
					Literal literalValue = (Literal) value;
					value = literalValue.getValue();
				}
				
				writeColumnData(entity, column, value, writeMethods);

			}
		}
		
		keyValue = convertKeyValue(keyValue, keyCol);
		
		//TODO: for 1.8 use putIfAbsent
		
		Object rootObject = connection.getSearchType().performKeySearch(ObjectUtil.getRecordName(keyCol), keyValue, executionContext) ;
				//env.performKeySearch(ObjectUtil.getRecordName(keyCol), keyValue, connection, executionContext);

		if (rootObject != null) {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21007, new Object[] {insert.getTable().getName(), keyValue}));
		}
		
		connection.add(keyValue, entity);

		return 1;

	}

	@SuppressWarnings("rawtypes")
	private int handleInsertChildObject(ObjectVisitor visitor, Object newEntity, Map<String, Method> writeMethods, Class<?> clzz) throws TranslatorException {

		ForeignKey fk = visitor.getForeignKey();

		String fkeyColNIS = fk.getNameInSource();
		String fkeyRefColumnName = visitor.getForeignKeyReferenceColName();
		
		// get the root method based on the foreign key, will be used to add the child
		Method rootClassWriteMethod = ClassRegistry.findMethod(this.getClassRegistry().getWriteClassMethods(this.connection.getCacheClassType().getName()), fkeyColNIS, this.connection.getCacheClassType().getName());
			
		List<ColumnReference> columns = visitor.getInsert().getColumns();
		List<Expression> values = ((ExpressionValueSource) visitor.getInsert()
				.getValueSource()).getValues();
		
		Object fkeyValue = null;
		
		// iterate over the columns and set data on newentity
		for (int i = 0; i < columns.size(); i++) {

			Column column = columns.get(i).getMetadataObject();
			Object value = values.get(i);

			// on a child object do not add the value for column that's the used in the foreign key,
			// which is already stored as the key value on the root object
			if (fkeyRefColumnName.equals(ObjectUtil.getRecordName(column)) ) {
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
				writeColumnData(newEntity, column, value, writeMethods);
			}
		}
		

		// need to find the parent object in order to add the child
		
		fkeyValue = convertKeyValue(fkeyValue, visitor.getPrimaryKeyCol());

		// don't perform a search when doing inserts for materialization
						
		Object rootObject = connection.getSearchType().performKeySearch(fkeyRefColumnName, fkeyValue, executionContext) ; 

		if (rootObject == null) {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21015, new Object[] {fkeyValue, connection.getCacheClassType().getSimpleName()}));
		}
		// read the child object from the root object based on the relationship 
		Object childrenObjects = this.evaluate(rootObject, fkeyColNIS, getClassRegistry().getReadScriptEngine(), connection.getCacheClassType());
			
		// first see if its a 1-to-many relationship
		// if it not of type (collection, map, or array), then
		// assume its a 1-to-1 relationship		
		Class<?> parmType = rootClassWriteMethod.getParameterTypes()[0];
		if (Collection.class.isAssignableFrom(parmType)) {
			Collection c = null;
			if (childrenObjects == null) {
				try {
					if (parmType.getConstructors() != null && parmType.getConstructors().length > 0) {
						c = (Collection) parmType.newInstance();
					} else {
						c = new ArrayList();
					}
				} catch (InstantiationException e) {
					throw new TranslatorException(e);
				} catch (IllegalAccessException e) {
					throw new TranslatorException(e);
				}
			} else {
				c = (Collection) childrenObjects;
			}
			c.add(newEntity);

			writeColumnData(rootObject, fkeyColNIS, c, rootClassWriteMethod);		
		} else if (parmType.isAssignableFrom(Map.class)) {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21008, new Object[] {visitor.getTableName()}));
			
		} else if (parmType.isArray()) {
			Object[] a = null;
			Object[] n = null;
			if (childrenObjects == null) {
				n = new Object[1];
				n[0] = newEntity;
			} else {
				a = (Object[]) childrenObjects;
				n = new Object[a.length + 1];
				int i = 0;
				for (Object o:a) {
					n[i] = o;
				}
				n[i] = newEntity;
			}

			writeColumnData(rootObject, fkeyColNIS, n, rootClassWriteMethod);		

		} else {

			// then assume its a 1-to-1 relationship
			writeColumnData(rootObject, fkeyColNIS, newEntity, rootClassWriteMethod);
		}		

		connection.update(fkeyValue, rootObject);
		
		return 1;

	}

	// Private method to actually do a delete operation. 
	private int handleDelete(Delete delete, ObjectVisitor visitor,  Class<?> clz) throws TranslatorException {
		
		// child table deletes not implemented yet
		if (visitor.getForeignKey() != null) {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21017, new Object[] {visitor.getTableName()}));
		}
	
		// if this is the root class (no foreign key), then for each object, obtain
		// the primary key value and use it to be removed from the cache
		Column keyCol = visitor.getPrimaryKeyCol();
		if (keyCol == null) {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21018, new Object[] {visitor.getTableName()}));
		}
		

		// Find all the objects that meet the criteria for deletion
		List<Object> toDelete = connection.getSearchType().performSearch(visitor, executionContext) ;
				//env.search(visitor, connection, executionContext);
		if (toDelete == null || toDelete.isEmpty()) {
			LogManager.logWarning(LogConstants.CTX_CONNECTOR, ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21013, new Object[] {visitor.getTableName(), visitor.getWhereCriteria()}));
			return 0;
		}
		int cnt = 0;
		try {
			ObjectScriptEngine scriptEngine =this.getClassRegistry().getReadScriptEngine();

			CompiledScript cs = scriptEngine.compile(ClassRegistry.OBJECT_NAME + "." +  ObjectUtil.getRecordName(keyCol));

			for (Object o : toDelete) {
				sc.setAttribute(ClassRegistry.OBJECT_NAME, o,
						ScriptContext.ENGINE_SCOPE);
				Object v = cs.eval(sc);

				v = convertKeyValue(v, keyCol);
				Object removed = connection.remove(v);
				if (removed == null) {
					if (connection.get(v) == null) {
						LogManager.logWarning(LogConstants.CTX_CONNECTOR, ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21013, new Object[] {visitor.getTableName(), v}));
					} 
					throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21012, new Object[] {visitor.getTableName(), v}));
						
				}
				++cnt;

			}

		} catch (ScriptException e1) {
			throw new TranslatorException(e1);
		}
		return cnt;
		
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
//						final String msg = ObjectPlugin.Util
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
//				final String msg = ObjectPlugin.Util
//						.getString("InfinispanUpdateExecution.scriptExecutionFailure"); //$NON-NLS-1$
//				throw new TranslatorException(e1, msg);
//			}

	}

	// Private method to actually do an update operation. 
	private int handleUpdate(Update update, ObjectVisitor visitor, ClassRegistry classRegistry,  Class<?> clz, Map<String, Method> writeMethods) throws TranslatorException {
		// child table lupdates/deletes not implemented yet
		if (visitor.getForeignKey() != null) {
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21017, new Object[] {visitor.getTableName()}));
		}
		
		// Find all the objects that meet the criteria for updating
		List<Object> toUpdate = connection.getSearchType().performSearch(visitor, executionContext) ;
				//env.search(visitor, connection, executionContext);
		
		if (toUpdate == null || toUpdate.size() == 0){
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
					"InfinispanUpdateExecution.update: no objects found to update based on - " + update.toString()); //$NON-NLS-1$
			return 0;
		}
		
		Column keyCol = null;
		
		// if fk exist, its assumed its a container class
		// don't want to use PK, cause it can exist on any table
		ForeignKey fk = visitor.getForeignKey();
		if (fk == null) {
			keyCol = visitor.getPrimaryKeyCol();
		} 		

		List<SetClause> updateList = update.getChanges();
		int cnt = 0;
		if (keyCol != null) {
			for (Object entity:toUpdate) {
				
				Object keyValue = evaluate(entity, ObjectUtil.getRecordName(keyCol), classRegistry.getReadScriptEngine(), clz);
				
				for (SetClause sc:updateList) {
					Column column = sc.getSymbol().getMetadataObject();
					Object value = sc.getValue();
					
					if ( keyCol.getName().equals(column.getName()) ) {
						throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21009, new Object[] {keyCol.getName(),visitor.getTableName()}));						
					}
					
					if (value instanceof Literal) {
						Literal literalValue = (Literal) value;
						value = literalValue.getValue();

					} 
					
					writeColumnData(entity, column, value, writeMethods);
			
				}
				
				connection.update( convertKeyValue(keyValue, keyCol), entity);
				++cnt;
			
			}
			
		} 
		return cnt;
	}

	@Override
	public void close() {
		super.close();
		this.command = null;
		sc = null;
		clz = null;
		result = null;
	}

	private Object evaluate(Object value, String columnName, ObjectScriptEngine scriptEngine, Class entityClass)
			throws TranslatorException {
		Method m = null;
		try {
			m = ClassRegistry.findMethod(scriptEngine.getMethodMap(entityClass), columnName, entityClass.getSimpleName());
		} catch (ScriptException e1) {
			throw new TranslatorException(e1);
		}
		
		sc.setAttribute(ClassRegistry.OBJECT_NAME, value,
				ScriptContext.ENGINE_SCOPE);
		try {
			CompiledScript cs = scriptEngine.compile(ClassRegistry.OBJECT_NAME
					+ "." + m.getName());
			final Object v = cs.eval(sc);
			return v;
		} catch (ScriptException e) {
			throw new TranslatorException(e);
		}

	}
	
	private  Object convertKeyValue(Object value, Column keyColumn )  throws TranslatorException {
		if (connection.getCacheKeyClassType() != null) {
			try {
				return DataTypeManager.transformValue(value,  connection.getCacheKeyClassType());
			} catch (TransformationException e) {
				throw new TranslatorException(e);
			}
		}
		
		return value;

	}
	
	private void writeColumnData(Object entity, Column column, Object value, Map<String, Method> writeMethods) throws TranslatorException {
		
			String nis = ObjectUtil.getRecordName(column);
	
			writeColumnData(entity, nis, value, writeMethods);
	}
	
	
	private void writeColumnData(Object entity, String nameInSource, Object value, Map<String, Method> writeMethods) throws TranslatorException {
			
			Method m = ClassRegistry.findMethod(writeMethods, nameInSource, entity.getClass().getSimpleName());
			writeColumnData(entity, nameInSource, value, m);
			
	}
	private void writeColumnData(Object entity, String nameInSource, Object value, Method m) throws TranslatorException {

			try {
				if (value == null) {
					ClassRegistry.executeSetMethod(m, entity, value);
					return;
				}
				final Class<?> sourceClassType = value.getClass();
				final Class<?> targetClassType = m.getParameterTypes()[0];
				if (targetClassType == null) {
					throw new TranslatorException("Program Error: invalid method " + m.getName() + ", no valid parameter defined on the expected set method");
				}
				if (targetClassType.isEnum()) {
					Object[] con = targetClassType.getEnumConstants();
					for (Object c:con) {
						if (c.toString().equalsIgnoreCase(value.toString())) {
							ClassRegistry.executeSetMethod(m, entity, c);
							return;
						}
					}
					throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21010, new Object[] {nameInSource}));
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
					PropertiesUtils.setBeanProperty(entity, nameInSource, value);
				}

			} catch (TranslatorException e) {
				throw e;
			} catch (Exception e) {
				throw new TranslatorException(e);
			}

	}
	
	protected ObjectVisitor createVisitor() {
        return new ObjectVisitor();

    }
	
}
