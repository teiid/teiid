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

package org.teiid.translator.coherence;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.coherence.visitor.CoherenceVisitor;


public abstract class SourceCacheAdapter {
  
  protected MetadataFactory metadataFactory = null;

	/*****************
	 * Methods for Adding the source metadata
	 *****************/
	
      protected Schema addSchema(String s) throws TranslatorException {
    	  Schema sc = new Schema();
    	  sc.setName(s);
    	  sc.setPhysical(true);
    	  metadataFactory.getMetadataStore().addSchema(sc);
    	  return sc;
    	  
      }
	  protected Table addTable(String t) throws TranslatorException {
		  return metadataFactory.addTable(t); //$NON-NLS-1$
	  }

	  protected KeyRecord addPrimaryKey(String name, List<String> columnNames, Table table) throws TranslatorException {
		  return metadataFactory.addPrimaryKey(name, columnNames, table); //$NON-NLS-1$
	  }

	  protected KeyRecord addForeignKey(String name, List<String> columnNames, Table pktable, Table table) throws TranslatorException {
		  return metadataFactory.addForiegnKey(name, columnNames, pktable, table);
	  }
	  
	  protected void addColumn(String columnName, String nameInSource, String dataType, Table t) throws TranslatorException {
		  Column c = metadataFactory.addColumn(columnName, dataType, t); //$NON-NLS-1$
		  c.setNameInSource(nameInSource);	  
	  }
	 
	  /**
	   * END of Methods for Adding source metadata
	   */
 
    /**
     * Called so the implementor can defined its table/column metadata
     * Use the methods @see #addTable and @see #addColumn.
     */
  public void addMetadata() throws TranslatorException {
	  
  }
  
  /**
   * Called to request the class name for the specified <code>objectName</code>
   * @param objectName is the name of the object for which the class name is being requested
   * @return String class name for the specified objectName
   * @throws TranslatorException
   */
//  public abstract String getMappedClass(String objectName) throws TranslatorException;
  
  /**
   * Called to translate the list of <code>objects</code> returned from the Coherence cache.
   * The implementor will use the <code>visitor</code> to obtain sql parsed information 
   * needed to understand the columns requested.  Then use the @see #retrieveValue method
   * in order to get the object value returned in the correct type to be passed in the
   * rows returned to the engine.
   */
  public List translateObjects(List objects, CoherenceVisitor visitor) throws TranslatorException {

		List row = null;
		
		List rows = new ArrayList();

		Map columnObjectMap = null;
		
		String[] attributeNames = visitor.getAttributeNames();
		Class[] attributeTypes = visitor.getAttributeTypes();

		
		// these pinpoint the column where a collection is found
		// and the name nodes it takes to traverse to get the collection
		//**********************************
		// NOTE: ONLY 1 COLLECTION CAN BE DEFINED IN A QUERY
		//**********************************
		String attributeNameForCollection = "COL_TAG";
		int attributeLocForCollection = -1;
		String attributeNamePrefixForCollection = null;
		List<String> attributeTokensForCollection = null;
		int collectionNodeDepth = -1;
		Set<String> collectionNames = new HashSet<String>();

		// 1st time thru, call to get the objects, if a collection is found, that is stored for
		// processing in the next step because it impacts the number rows returned
		for (Iterator<Object> it = objects.iterator(); it.hasNext();) {
			// each object represent 1 row, but will be expanded, if a collection is found in its results 
			Object o = (Object) it.next();
			columnObjectMap = new HashMap();
			
			for (int i=0; i<attributeNames.length; i++) {
				final String n = attributeNames[i];
				
				List<String> tokens = StringUtil.getTokens(n, ".");
				
				final ParmHolder holder = ParmHolder.createParmHolder(visitor.getTableName(), tokens, attributeTypes[i]);

				Object value =  retrieveValue(holder, o, 0);
				  if (holder.isCollection) {
					  if (attributeLocForCollection == -1) {
						  // if a collection type has not been found, then identify it
						  attributeLocForCollection = i;
						  attributeTokensForCollection = tokens;
						  collectionNodeDepth = holder.collectionNodeDepth;
						  for (int x = 0; x <= holder.collectionNodeDepth; x++) {
							  if (x > 0) {
								  attributeNamePrefixForCollection+=".";
							  }
							  attributeNamePrefixForCollection+=tokens.get(x);
						  }
						  columnObjectMap.put(attributeNameForCollection, value);
						  collectionNames.add(n);
						  
					  } else if (collectionNodeDepth == holder.collectionNodeDepth) {
						  // if a collection was requested in another column, check to see if the
						  // same node method was called, if not, then this represents a different collection being retrieved
						 String a = attributeTokensForCollection.get(collectionNodeDepth);
						 String b = tokens.get(holder.collectionNodeDepth);
						 if (!b.equals(a)) {
							 throw new TranslatorException("Query Error: multiple collections found between " + a + " and " + b +", only 1 is supported per query" );
					  
						 }
						 collectionNames.add(n);
					  }
					  
				  } else {

					  columnObjectMap.put(n, value);
				  } // end of isCollection
				
			}
			
		
			if (attributeLocForCollection != -1) {
				Object colObj = columnObjectMap.get(attributeNameForCollection);
				Iterator colIt = null;
				  if (colObj.getClass().isArray()) {
				      List objRows = Arrays.asList((Object[]) colObj);
				      colIt = objRows.iterator();
				  
				  } else if (colObj instanceof Collection) { 
				      Collection objRows = (Collection) colObj;
				      colIt = objRows.iterator();
				      
				  } else if (colObj instanceof Map) {
					  Map objRows = (Map) colObj;
					  colIt = objRows.values().iterator();
					  
				  } else {
					  throw new TranslatorException("Program Error: A container type of object is unsupported: " + colObj.getClass().getName());
				  }
				  				  
				  
					for (Iterator<Object> objit = colIt; colIt.hasNext();) {
						Object colt = (Object) colIt.next();

						row = new ArrayList<Object>(attributeNames.length);
						
						for (int i=0; i<attributeNames.length; i++) {
							String n = attributeNames[i];
							
							if (collectionNames.contains(n)) {
								// when a collection is found, need to find the value for the row
								// in order to do that, need to pick where retrieve process left off
								List<String> tokens = StringUtil.getTokens(n, ".");
								Object colvalue = getValue(visitor.getTableName(), tokens, attributeTypes[i], colt, collectionNodeDepth + 1);
								row.add(colvalue);
								
							} else {
								row.add(columnObjectMap.get(n));
							}
							
						}
						rows.add(row);
					}
			
			} else {
				row = new ArrayList<Object>(attributeNames.length);
				
				for (int i=0; i<attributeNames.length; i++) {
					String n = attributeNames[i];
					
					Object attributeObject = columnObjectMap.get(n);
					
					row.add(attributeObject);
			
				}
				rows.add(row);

			}
			
			columnObjectMap.clear();
			collectionNames.clear();
			attributeLocForCollection = -1;
			// don't reset the following because, once set, they should be the same for all
			//  attributeNamePrefixForCollection
			//  collectionNodeDepth
			
			
		}
			
	return rows;

  }
  
  protected void setMetadataFactory(MetadataFactory metadataFactory) throws TranslatorException {
    	this.metadataFactory = metadataFactory;
    	addMetadata();
    }

  public Object getValue(String tableName, List<String> nameTokens, Class type, Object cachedObject, int level ) throws TranslatorException {
		final ParmHolder holder = ParmHolder.createParmHolder(tableName, nameTokens, type);
		Object value =  retrieveValue(holder, cachedObject, level);
		return value;

  }

		
private Object retrieveValue(ParmHolder holder, Object cachedObject, int level) throws TranslatorException {
	// do not retrieve a value for these types
	 if (cachedObject.getClass().isArray() || cachedObject instanceof Collection || cachedObject instanceof Map) {
		 return cachedObject;
	 }

	 final Class objectClass = cachedObject.getClass();
	 
	 final String columnName = holder.nameNodes.get(level);

	 boolean atTheBottom = false;
	 
	 if (holder.nodeSize == (level + 1)) atTheBottom = true;
	 
	 try {
		 String methodName = null;
		 // only the last parsed name can be where the boolean call can be made
		 // example:  x.y.z     z will be where "is" is called
		 //			  or x	    x could be where "is" is called 
		 if (atTheBottom && holder.attributeType == Boolean.class) {
			 methodName = "is" + columnName;
		 } else {
			 methodName = "get" + columnName;
		 }

		 final Method m = findBestMethod(objectClass, methodName, null);
		 
		 final Object value = m.invoke(cachedObject, null);
		 
		 // if an array or collection, return, this will be processed after all objects are obtained
		 // in order the number of rows can be created
		 if (value == null) {
			 return null;
		 }
		  if (value.getClass().isArray() || value instanceof Collection || value instanceof Map) {
			  holder.setCollection(level);  
//			  System.out.println("Found Collection: " + methodName);
			  return value;
		  }
			 
			 if (atTheBottom) {
				 return value;
			 }

		  return retrieveValue(holder, value, ++level);

		  
//		 	LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Got value " + value); //$NON-NLS-1$
		 	
			 
	 } catch (InvocationTargetException x) {
		 Throwable cause = x.getCause();
		 System.err.format("invocation of %s failed: %s%n",
				 "get" + columnName, cause.getMessage());
		 LogManager.logError(LogConstants.CTX_CONNECTOR, "Error calling get" + columnName + ":" + cause.getMessage());
		 return null;
	 } catch (Exception e) {
		 e.printStackTrace();
		 throw new TranslatorException(e.getMessage());
	 }
  }

  public Object createObjectFromMetadata(String metadataName) throws TranslatorException {
//	  String mappedClass = getMappedClass(metadataName);
	  return createObject(metadataName);
	  
  }

  private Object createObject(String objectClassName) throws TranslatorException {
		try {
			
			return  ReflectionHelper
					.create(objectClassName,
							null, null);
		} catch (Exception e1) {
			throw new TranslatorException(e1);
		}
	  
  }
  
  public Object setValue(String tableName, String columnName, Object cachedObject, Object value, Class classtype) throws TranslatorException {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Adding value to attribute: " + columnName); //$NON-NLS-1$
		
	 try {
		 ArrayList argTypes = new ArrayList(1);
		 argTypes.add(classtype);
		 Method m = findBestMethod(cachedObject.getClass(), "set" + columnName, argTypes);
		 
		 Class[] setTypes = m.getParameterTypes();
		 
		 
		 Object newValue = null;
		 if (value instanceof Collection || value instanceof Map || value.getClass().isArray() ) {
			 newValue = value;
		 } else {
			 newValue = DataTypeManager.transformValue(value, setTypes[0]);
		 }
	//	 Object newValue = getArg(m, value);
			 m.invoke(cachedObject, new Object[] {newValue});
		 	
		 	LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Set value " + value); //$NON-NLS-1$
		 return newValue;
	 } catch (InvocationTargetException x) {
		 x.printStackTrace();
		 Throwable cause = x.getCause();
		 System.err.format("invocation of %s failed: %s%n",
				 "set" + columnName, cause.getMessage());
		 LogManager.logError(LogConstants.CTX_CONNECTOR, "Error calling set" + columnName + ":" + cause.getMessage());
		 return null;
	 } catch (Exception e) {
		 e.printStackTrace();
		 throw new TranslatorException(e.getMessage());
	 }

}  
  

  private  Object getArg(Method m, int value) {
	  return Integer.valueOf(value);
  }
  private  Object getArg(Method m, double value) {
	  return Double.valueOf(value);
  }
  private  Object getArg(Method m, long value) {
	  return Long.valueOf(value);
  }
  private  Object getArg(Method m, float value) {
	  return Float.valueOf(value);
  }
  private  Object getArg(Method m, short value) {
	  return Short.valueOf(value);
  }
  private  Object getArg(Method m, boolean value) {
	  return Boolean.valueOf(value);
  }
  
  private  Object getArg(Method m, Long value) {
	  return value.longValue();
  }

  private  Object getArg(Method m, Object value) throws Exception {
	  return value;
  }

	
  
//  private static Object retrieveValue(Integer code, Object value) throws Exception {
//      if(code != null) {
//          // Calling the specific methods here is more likely to get uniform (and fast) results from different
//          // data sources as the driver likely knows the best and fastest way to convert from the underlying
//          // raw form of the data to the expected type.  We use a switch with codes in order without gaps
//          // as there is a special bytecode instruction that treats this case as a map such that not every value 
//          // needs to be tested, which means it is very fast.
//          switch(code.intValue()) {
//              case INTEGER_CODE:  {
//                  return Integer.valueOf(value);
//              }
//              case LONG_CODE:  {
//                  return Long.valueOf(value);
//              }                
//              case DOUBLE_CODE:  {
//                  return Double.valueOf(value);
//              }                
//              case BIGDECIMAL_CODE:  {
//                  return value; 
//              }
//              case SHORT_CODE:  {                  
//                  return Short.valueOf(value);
//              }
//              case FLOAT_CODE:  {
//                  return Float.valueOf(value);
//              }
//              case TIME_CODE: {
//          		return value;
//              }
//              case DATE_CODE: {
//          		return value;
//              }
//              case TIMESTAMP_CODE: {
//          		return value;
//              }
//  			case BLOB_CODE: {
//  					return value;
//  			}
//  			case CLOB_CODE: {
//  					return value;
//  			}  
//  			case BOOLEAN_CODE: {
//  				return Boolean.valueOf(value);
//  			}
//          }
//      }
//
//      return value;
//  }
  
  
  private  Method findBestMethod(Class objectClass, String methodName, List argumentsClasses) throws SecurityException, NoSuchMethodException {
      ReflectionHelper rh = new ReflectionHelper(objectClass);
      
      if (argumentsClasses == null) {
          argumentsClasses = Collections.EMPTY_LIST;
      }
      Method m = rh.findBestMethodWithSignature(methodName, argumentsClasses);
      return m;
     
  }


}

final class ParmHolder {
	static ParmHolder holder = new ParmHolder();
	String tableName;
	List<String> nameNodes;
	Class attributeType;
	int nodeSize;
	
	// these parameters are use when the retrieved object is a collection type
	// the node name path need to be captured
	// 
	boolean isCollection = false;
	String nodeNamePathToGeCollection=null;
	int collectionNodeDepth = -1;
	
	
	private ParmHolder() {
		
	}
	
	static ParmHolder createParmHolder(String tablename, List<String> parsedAttributeName,  Class<?> type) {
			
		holder.tableName = tablename;
		holder.nameNodes = parsedAttributeName;
		holder.attributeType = type;
		holder.nodeSize = parsedAttributeName.size();
		holder.isCollection = false;
		holder.nodeNamePathToGeCollection=null;
		holder.collectionNodeDepth = -1;
		
		return holder;
	}
	
	void setCollection(int depth) {
		
		isCollection = true;
		collectionNodeDepth = depth;
		
	}
	
}
