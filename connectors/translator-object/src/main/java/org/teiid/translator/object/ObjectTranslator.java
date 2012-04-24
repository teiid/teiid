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
 	 *				Object X
 	 *					Attributes:  Name
 	 *								 Addresses (List)
 	 *								 Phones (List)
 	 *         Object X is mapped to Table A
 	 *								 Name
 	 *								 Street
 	 *								 City
 	 *								 State
 	 *								 Phone_Number
 	 *
 	 *		To model this correctly, you would create 2 Tables (A and B):
 	 *					Table A
 	 *							Name, Street, City, State
 	 *					Table B
 	 *							Name, Phone_Number
 	 *
 	 *		If the user wants a cross-product result, then allow the Teiid engine to perform that logic, but the Translator
 	 *		will only traverse one container path per result set.  I say container path, because it will be possible for 
 	 *		an object in a container to define another container object, and so on.  Theoretically, there is no depth limit. 	
	 * <li></li>
	 * @param objects is the List of objects from the cache
	 * @param projections are the columns to be returned in the result set
	 * @param objectManager is responsible for providing the object methods used for traversing an object and/or 
	 * 			obtaining the data from an object
	 * @return List<List<?>> that represent the rows and columns in the result set
	 */
	public static List<List<?>> translateObjects(List<Object> objects, ObjectProjections projections, ObjectMethodManager objectManager) throws TranslatorException {

		projections.throwExceptionIfFound();
		
		List<List<?>> rows = new ArrayList<List<?>>(objects.size());			
		
		// if no container objects required in the results, then
		// perform simple logic for building a row
		int numCols = projections.columnNamesToUse.length;

		if (!projections.hasChildren()) {
			for (Iterator<Object> it = objects.iterator(); it.hasNext();) {
				// each object represent 1 row
				Object o = (Object) it.next();
				
				List<Object> row  = new ArrayList<Object>(numCols);
				for (int i = 0; i < numCols; i++) {								
					Object value = getValue(o, i, 0, projections, objectManager);
					
					row.add(value);
				}				
	
				rows.add(row);
			}
			
			return rows;
		}
				
		for (Iterator<Object> it = objects.iterator(); it.hasNext();) {
			// each object represent 1 row, but will be expanded, if a
			// collection is found in its results
			Object o = (Object) it.next();
							
			List<List<Object>> containerRows = processContainer(o, 0, projections, objectManager);

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
			int level, ObjectProjections projections, ObjectMethodManager objectManager) throws TranslatorException {
		
		List<List<Object>> containerRows = new ArrayList<List<Object>>();
		// if there another container depth, then process it first
		// this will be recursive 
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
		
		if (containerRows.isEmpty()) {
			containerRows = new ArrayList<List<Object>>(1);
			List<Object> row  = new ArrayList<Object>(projections.columnNamesToUse.length);
			for (int i = 0; i < projections.columnNamesToUse.length; i++) {	
				// the column must have as many nodes as the level being processed
				// in order to obtain the value at the current level
				if (projections.nameNodes[i].size() >= (level + 1)) {  // level is zero based
					Object value = getValue(parentObject, i, level, projections, objectManager);
				
					row.add(value);
				} else {
					row.add(null);
				}
			}				

			containerRows.add(row);	
			return containerRows;
			
		}
		
	
		List<List<Object>> expandedRows = new ArrayList<List<Object>>();
		for (List<Object> row: containerRows) {
						
				List<Object> newrow  = new ArrayList<Object>(projections.columnNamesToUse.length);

				for (int col=0; col<projections.columnNamesToUse.length; ++col) {
					// only make method calls for columns that are being processed at the same level,
					// columns at other node depths will be loaded when its level is processed
					
					final Object colObject = row.get(col);

					if (projections.nodeDepth[col] == level) {
												
						if (colObject != null) throw new TranslatorException("Program Error:  column object was not null for column " + projections.columnNamesToUse[col] + " at level " + level);
		
						final Object value = getValue(parentObject, col, level, projections, objectManager);
					
						newrow.add(value);
						
					} else {					
						newrow.add(colObject);
					}

				}

				expandedRows.add(newrow);
		}
		return expandedRows;
	}
	
	private static Object getValue(Object cachedObject, int columnIdx, int methodIdx, ObjectProjections projections, ObjectMethodManager objectManager) throws TranslatorException {


		// only the last parsed name can be where the boolean call can be made
		// example: x.y.z z will be where "is" is called
		// or x x could be where "is" is called
		Class<?> clzType = projections.columns[columnIdx].getJavaType();


		Object value = null;
			
		if (clzType != null && clzType == Boolean.class) {
			final String methodName = objectManager.formatMethodName(
					ObjectMethodManager.IS, projections.nameNodes[columnIdx].get(methodIdx) );
			
			value = objectManager.getIsValue(
					methodName, cachedObject);	
		} else {
			final String methodName = objectManager.formatMethodName(
					ObjectMethodManager.GET, projections.nameNodes[columnIdx].get(methodIdx) );
			
			value = objectManager.getGetValue(
					methodName, cachedObject);
			
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