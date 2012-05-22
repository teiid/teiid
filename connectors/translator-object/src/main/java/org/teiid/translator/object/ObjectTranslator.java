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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
import org.teiid.core.types.DataTypeManager.DefaultTypeCodes;
import org.teiid.core.types.TransformationException;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;

/**
 * The ObjectTranslator is the responsible for introspection of an object and turning it into rows.
 * 
 * This will use reflections, based on model metadata, to invoke methods on the objects
 * 
 * @author vhalbert
 * 
 */
public class ObjectTranslator {
	
	public enum CONTAINER_TYPE {
		COLLECTION,
		ARRAY,
		MAP
	}

	/**
	 * <p>
	 * Called to translate the list of <code>objects</code> returned from the
	 * cache. The <code>projections</code> will be used to drive what information on
	 * the objects that will be obtained.
	 * </p>
	 * <p>
	 * The assumptions in converting objects to relational are as follows:
	 * <li>its assumed the objects have been normalized when modeled, not de-normalized.  The object is de-normalized
	 * 		if table or view is defined to have multiple container objects (ie., Maps, List, etc.) returned
	 * 		in the same source query. 
	 * 		<p> Example:
 	 *				Object Person
 	 *					Attributes:  Name 
 	 *								 Addresses (List)
 	 *								 Phones (List)
 	 *
 	 *					Addresses contained Address Object(s)
 	 *								 Street
 	 *								 City
 	 *								 State
 	 *								 Zip
 	 *
 	 *					Phones contained Phone Object(s)
 	 *								 Phone_Number
 	 *
 	 *
 	 *		To model this correctly, you would create 3 Tables (Person, Address, Phone):
 	 *					Table Person
 	 *							Name (String)
 	 *							Addresses (Object)
 	 *							Phones (Object)
 	 * 							PK-Name
 	 *					Table Address
 	 *							Name,
 	 *							Street
 	 *							City
 	 *							State
 	 *							Zip
 	 *							FK-PersonName
 	 * 
 	 * 					Table Phone
 	 * 							Name
 	 *							Phone_Number
 	 *							FK-PersonName
 	 *					
 	 *
 	 *	 	The recommendation now is to create a view for each logical set (i.e., PersonAddresses and PersonPhoneNumbers).		
 	 *
 	 *		If the user wants a cross-product result, then allow the Teiid engine to perform that logic, but the Translator
 	 *		will only traverse one container path per result set.  I say container path, because it will be possible for 
 	 *		an object in a container to define another container object, and so on.  Theoretically, there is no depth limit. 	
	 * <li></li>
	 * @param objects is the List of objects from the cache
	 * @param visitor are the columns to be returned in the result set
	 * @param objectManager is responsible for providing the object methods used for traversing an object and/or 
	 * 			obtaining the data from an object
	 * @return List<List<?>> that represent the rows and columns in the result set
	 */
	public static List<List<Object>> translateObjects(List<Object> objects, ObjectVisitor visitor, ObjectMethodManager objectManager) throws TranslatorException {

		visitor.throwExceptionIfFound();
		
		List<List<Object>> rows = new ArrayList<List<Object>>(objects.size());			
		
		// if no container objects required in the results, then
		// perform simple logic for building a row
		int numCols = visitor.columnNamesToUse.length;

		if (!visitor.hasChildren()) {
			for (Iterator<Object> it = objects.iterator(); it.hasNext();) {
				// each object represent 1 row
				Object o = (Object) it.next();
				
				boolean includeRow = true;
				
				List<Object> row  = new ArrayList<Object>(numCols);
				for (int i = 0; i < numCols; i++) {		
					includeRow = addValueToRow(o, i, 0, visitor, objectManager, row);
					if (!includeRow) {
						row.clear();
						break;
					}
				}				
	
				if (includeRow) {
					rows.add(row);
				}
			}
			
			return rows;
		}
				
		for (Iterator<Object> it = objects.iterator(); it.hasNext();) {
			// each object represent 1 row, but will be expanded, if a
			// collection is found in its results
			Object o = (Object) it.next();
							
			List<List<Object>> containerRows = processContainer(o, 0, visitor, objectManager);

			rows.addAll(containerRows);
		}

		return rows;
	}
	
	/**
	 * This is a recursively called method to traverse the container calls.  The logic
	 * will traverse to the last node in the node names and as it returns up the chain 
	 * of names, it will build the rows.
	 * 
	 * @param holder
	 * @param cachedObject
	 * @param level
	 * @param objectManager
	 * @return
	 * @throws TranslatorException
	 */
	@SuppressWarnings("unchecked")
	private static List<List<Object>> processContainer(Object parentObject,
			int level, ObjectVisitor projections, ObjectMethodManager objectManager) throws TranslatorException {
		
		List<List<Object>> containerRows = new ArrayList<List<Object>>();
		// if there is another container depth, then process it first
		// this will be recursive to get to the bottom child and return
		// back up the chain, expanding the rows for each child
		if (level < projections.childrenDepth) {
			String containerMethodName = projections.childrenNodes.get(level);
			
			final String methodName = objectManager.formatMethodName(
					ObjectMethodManager.GET, containerMethodName);
			
			Object containerValue = objectManager.getGetValue(
					methodName, parentObject);

			if (containerValue != null) {
				
				CONTAINER_TYPE type = getContainerType(containerValue);
				

				Iterator<Object> colIt = null;
				switch (type) {
				case ARRAY:
					List<Object> listRows = Arrays.asList((Object[]) containerValue);
					colIt = listRows.iterator();
					break;
				case COLLECTION:
					@SuppressWarnings("rawtypes")
					Collection<Object> colRows = (Collection) containerValue;
					colIt = colRows.iterator();
					break;
				case MAP:
					@SuppressWarnings("rawtypes")
					Map<?,Object> mapRows = (Map) containerValue;
					colIt = mapRows.values().iterator();
					break;
				default:
							
				}
				
				if (colIt != null) {
					while (colIt.hasNext()) {
						Object colt = (Object) colIt.next();
						
						final List<List<Object>> rows = processContainer(colt, level+1, projections, objectManager);
						containerRows.addAll(rows);
					} 
				} else {
					final List<List<Object>> rows = processContainer(containerValue, level+1, projections, objectManager);

					containerRows.addAll(rows);
				}
			}

		}
		
		// if no children object were needed,which would expand the number of rows,
		// then this one object will be the one row returned
		if (containerRows.isEmpty()) {
			List<Object> row  = new ArrayList<Object>(projections.columnNamesToUse.length);
			boolean includeRow = true;
			for (int i = 0; i < projections.columnNamesToUse.length; i++) {	
				// the column must have as many nodes as the level being processed
				// in order to obtain the value at the current level
				if (projections.nameNodes[i].size() >= (level + 1)) {  // level is zero based
					includeRow = addValueToRow(parentObject, i, level, projections, objectManager, row);
					if (!includeRow) {
						row.clear();
						break;
					}
					
				} else {
					row.add(null);
				}
			}				

			if (includeRow) {
				containerRows.add(row);	
			}
			return containerRows;
			
		}
		
	
		List<List<Object>> expandedRows = new ArrayList<List<Object>>();
		for (List<Object> row: containerRows) {
						
				List<Object> newrow  = new ArrayList<Object>(projections.columnNamesToUse.length);

				boolean includeRow = true;
				for (int col=0; col<projections.columnNamesToUse.length; ++col) {
					// only make method calls for columns that are being processed at the same level,
					// columns at other node depths will be loaded when its level is processed
					
					final Object colObject = row.get(col);

					if (projections.nodeDepth[col] == level) {
												
						// this should not happen, but just in case
						if (colObject != null) throw new TranslatorException("Program Error:  column object was not null for column " + projections.columnNamesToUse[col] + " at level " + level);
								
						includeRow = addValueToRow(parentObject, col, level, projections, objectManager, newrow);
						if (!includeRow) {
							newrow.clear();
							break;
						}
						
					} else {					
						newrow.add(colObject);
					}

				}

				if (includeRow) {
					expandedRows.add(newrow);
				}
		}
		return expandedRows;
	}
	
	/*
	 * Return false when the row should be excluded from the results
	 */
	private static boolean addValueToRow(Object cachedObject, int columnIdx, int methodIdx, ObjectVisitor visitor, ObjectMethodManager objectManager, List<Object> row) throws TranslatorException {

		// only the last parsed name can be where the boolean call can be made
		// example: x.y.z z will be where "is" is called
		// or x x could be where "is" is called
		Column c = visitor.columns[columnIdx];
		Class<?> clzType = c.getJavaType();
		
		Object value = getValue(cachedObject, visitor.nameNodes[columnIdx].get(methodIdx), clzType, objectManager);
		
		if (visitor.hasFilters()) {
			SearchCriterion sc = visitor.getFilters().get(c.getFullName());

				while (sc != null) {
						Object searchValue = null;
						try {
							if (sc.getValue().getClass().equals(clzType) ) {
								searchValue = sc.getValue();

								if (searchValue != null && searchValue.equals(value)) {
								} else {
									return false;
								}								
							} else if (DataTypeManager.isTransformable(sc.getValue().getClass(), clzType)) {
								searchValue = DataTypeManager.getTransform(sc.getValue().getClass(), clzType).transform(sc.getValue());
								// if the filter matches, then return false to indicate this row is excluded
								if (searchValue.equals(value)) {
								} else {
									return false;
								}
							}  else {
								return false;
							}
						} catch (TransformationException e) {
							// TODO Auto-generated catch block
							throw new TranslatorException(e);
						}
						
					sc = sc.getAddCondition();
					
				}
				
		}
		row.add(value);
		
		return true;
	}
		

	private static Object getValue(Object cachedObject, String columnName, Class<?> clzType, ObjectMethodManager objectManager) throws TranslatorException {
		Object value = null;
		Class<?> dataTypeClass = DataTypeManager.getDataTypeClass(cachedObject.getClass().getName());
		
		if (cachedObject.getClass().equals(clzType)) {
			return cachedObject;
		}

		// if the class is not a native type, but the POJO object, then
		// call the method on the class to get the value
		if (dataTypeClass == DefaultDataClasses.OBJECT) {
			if (clzType != null && clzType == Boolean.class) {
				final String methodName = objectManager.formatMethodName(
						ObjectMethodManager.IS, columnName );
				
				value = objectManager.getIsValue(
						methodName, cachedObject);	
			} else {
				final String methodName = objectManager.formatMethodName(
						ObjectMethodManager.GET, columnName );
				
				value = objectManager.getGetValue(
						methodName, cachedObject);
				
			}			
		} else {
		
			int datatype = DataTypeManager.getTypeCode(cachedObject.getClass());		
	
			switch (datatype) {
			case DefaultTypeCodes.OBJECT:
			case DefaultTypeCodes.CLOB:
			case DefaultTypeCodes.BLOB:
				
				break;
	
			default:
				
				try {
					if (DataTypeManager.isTransformable(cachedObject.getClass(), clzType)) {
						value = DataTypeManager.getTransform(cachedObject.getClass(), clzType).transform(cachedObject);
					} else {
						return cachedObject;
					}
				} catch (TransformationException e) {
					// TODO Auto-generated catch block
					throw new TranslatorException(e);
				}				
				
				break;
			}
		}
		
		return value;
	}


	private static CONTAINER_TYPE getContainerType(Object o) {
		if (o.getClass().isArray()) {
			return ObjectTranslator.CONTAINER_TYPE.ARRAY;
		}
		
		if (o instanceof Collection) {
			return  ObjectTranslator.CONTAINER_TYPE.COLLECTION;
		} 
		
		if (o instanceof Map) {
			return  ObjectTranslator.CONTAINER_TYPE.MAP;
		}
		return null;
	}

}