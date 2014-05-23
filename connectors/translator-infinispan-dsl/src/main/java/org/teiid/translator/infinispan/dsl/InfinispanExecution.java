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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.teiid.core.util.StringUtil;
import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.eval.TeiidScriptEngine;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

/**
 * Execution of the SELECT Command
 */
public class InfinispanExecution implements ResultSetExecution {

	private static final String OBJECT_NAME = "o"; //$NON-NLS-1$
	protected Select query;
	protected InfinispanConnection connection;
//	private ArrayList<CompiledScript> projects;
	private Object[] projects;
	private ScriptContext sc = new SimpleScriptContext();
	private static TeiidScriptEngine scriptEngine = new TeiidScriptEngine();
	private Iterator<Object> resultsIt = null;
	private Iterator<Object> cacheResultsIt = null;
	private InfinispanExecutionFactory factory;
	private ExecutionContext executionContext;
	private Map<Object, CompiledScript> depthScripts;
	private boolean nodeDepth = false;
//	private List[] depthColumns = null;
	private int colSize = 0;

	public InfinispanExecution(Select query, RuntimeMetadata metadata,
			InfinispanExecutionFactory factory, InfinispanConnection connection, ExecutionContext executionContext) throws TranslatorException {
		this.factory = factory;
		this.query = query;
		this.connection = connection;
		this.executionContext = executionContext;

		colSize = query.getDerivedColumns().size();
		
//		projects = new ArrayList<CompiledScript>(colSize);
		projects = new Object[colSize];
		int col = 0;
		for (DerivedColumn dc : query.getDerivedColumns()) {
			ColumnReference cr = (ColumnReference) dc.getExpression();
			String name = null;
			if (cr.getMetadataObject() != null) {
				Column c = cr.getMetadataObject();
				name = getNameInSource(c);
			} else {
				name = cr.getName();
			}
				if (name.equalsIgnoreCase("this")) { //$NON-NLS-1$
					// the object in cache is being requested
					projects[col] = null;
//					projects.add(null);
				} else if (name.indexOf(".") > 0)  {
					
//					if (depthColumns == null) {
//						depthColumns = new List[colSize];
//					}
					// inner classes are being accessed
					List<String> nodes = StringUtil.split(name, ".");
					
					projects[col] = nodes;
					nodeDepth = true;
//					// determine the max depth to go, minus the attribute
//					if (nodes.size() -1 > nodeDepth) {
//						nodeDepth = nodes.size() -1;
//					}
//					try {
//						projects.add(scriptEngine.compile(OBJECT_NAME + "." +  name));
					//nodes.get(nodes.size() -1))); //$NON-NLS-1$
//						depthColumns[col] =  nodes;
//					} catch (ScriptException e) {
//						throw new TranslatorException(e);
//					}
					
					
//					depthScripts.put(i, scriptEngine.compile(OBJECT_NAME + "." + nodes.get(i)) );
//					// subtract 1 so that the attribute isn't included in the depth
//					int newDepth = nodes.size() - 1;
//					// check if a deeper search is requested than has been currently detected
//					if ( newDepth > nodeDepth) {
//						for (int i=0; i< newDepth; i++) {
//							try {
//								depthScripts.put(i, scriptEngine.compile(OBJECT_NAME + "." + nodes.get(i)) );
//							} catch (ScriptException e) {
//								throw new TranslatorException(e);
//							} 
//						}	
//						nodeDepth = newDepth;
//					}
					// do include the method node, which is the last node

					
					
				} else {
					try {
						projects[col] = scriptEngine.compile(OBJECT_NAME + "." + name);
//						projects.add(scriptEngine.compile(OBJECT_NAME + "." + name)); //$NON-NLS-1$
					} catch (ScriptException e) {
						throw new TranslatorException(e);
					}
				}
			col++;
		}
	}

	@Override
	public void execute() throws TranslatorException {

		LogManager.logTrace(LogConstants.CTX_CONNECTOR,
				"InfinsipanExecution command:", query.toString(), "using connection:", connection.getClass().getName()); //$NON-NLS-1$ //$NON-NLS-2$

		String nameInSource = getNameInSource(((NamedTable)query.getFrom().get(0)).getMetadataObject());
	    
	    List<Object> results = factory.search(query, nameInSource, connection, executionContext);
	    
		if (results == null) {
			results = Collections.emptyList();
		}
		LogManager.logDetail(LogConstants.CTX_CONNECTOR,
				"InfinsipanExecution number of returned objects is :", results.size()); //$NON-NLS-1$

		this.resultsIt = results.iterator();
	}

	@Override
	public List<Object> next() throws TranslatorException,
			DataNotAvailableException {
		if (cacheResultsIt != null && cacheResultsIt.hasNext()) {
			// return the next row in the cache 
			return (List<Object>) cacheResultsIt.next();
		} 
		
		cacheResultsIt = null;
		
		// process the next object in the search result set
		if (resultsIt.hasNext()) {
			List<Object> r = new ArrayList<Object>(colSize);
			Object o = resultsIt.next();
			sc.setAttribute(OBJECT_NAME, o, ScriptContext.ENGINE_SCOPE);
			
			if (! nodeDepth) {
				for (int i = 0; i < colSize; i++) {
//				for (CompiledScript cs : this.projects) {
					if (projects[i] == null) {
						r.add(o);
						continue;
					}
					try {
						CompiledScript cs = (CompiledScript) projects[i];
						r.add(cs.eval(sc));
					} catch (ScriptException e) {
						throw new TranslatorException(e);
					}
				}				
				
			} else {
//				Object[] depthValues = new Object[colSize];
//				// process each column that is in subclasses
//				for (int c = 0; c < colSize; c++) {
//					if (projects[c] != null) {
//						List<String> nodes = (List<String>) depthColumns[c];
//						List<Object> n = processNode(nodes, 0);
//						depthValues[c] = n;
//				//		depthColumns[c] = String.valueOf(nodes.size() -1);
//					}
//				}
//				int col=0;
//				for (CompiledScript cs : this.projects) {
//					// if compiledScript is null and not a column that has depth, then return object
//					if (cs == null && depthColumns[col] == null) {
//						r.add(o);
//						continue;
//					}
//					try {
//						Object eo = cs.eval(sc);
//						if (eo instanceof List) {
//							List c = (List) eo;
//							for (Object obj:c) {
//								List<Object> rtn = processNode(c);
//							}							
//						} else {
//							r.add(eo);
//						}
//					} catch (ScriptException e) {
//						throw new TranslatorException(e);
//					}
//					col++;
//				}
			}
			return r;
		}
		return null;
	}
	
	private List<Object> processNode(List<String> nodes, int depth) throws TranslatorException {
		List<Object> r = null;
		try {

			CompiledScript cs = scriptEngine.compile(OBJECT_NAME + "." +  nodes.get(depth));
			Object o = cs.eval(sc);
			//Class<?> type
//			if (type.isPrimitive()) {
//				            switch (type.getName().charAt(0)) {
//				                case 'b':
//				                    return (type == boolean.class) ? BOOLEAN : BYTE;
//					                case 'c':
//					                    return CHAR;
//					                case 'd':
//				                    return DOUBLE;
//					                case 'f':
//					                    return FLOAT;
//				                case 'i':
//				                    return INT;
//				                case 'l':
//				                    return LONG;
//					                case 's':
//				                    return SHORT;
//				           }
//				        }
			
//			if (Collection.class.isAssignableFrom(type))
//				            return COLLECTION;
//			        if (Map.class.isAssignableFrom(type))
//				           return MAP;
//				        if (type.isArray())
//				            return ARRAY;
//		     if (Enum.class.isAssignableFrom(type))
//		    	 	            return ENUM;
//		      if (Calendar.class.isAssignableFrom(type))
//		    	  	            return CALENDAR;
			
			if (o instanceof List) {
				List c = (List) o;
				for (Object obj:c) {
					List<Object> rtn = processNode(c);
				}							
			} else {
				r = new ArrayList<Object>(1);
				r.add(o);

			}
		} catch (ScriptException e) {
			throw new TranslatorException(e);
		}
		return r;
	}
	private List<Object> processNode(Object eo) throws TranslatorException {
			if (eo instanceof List) {
				List c = (List) eo;
				for (Object o:c) {
					List<Object> rtn = processNode(c);
					return rtn;
				}
				return Collections.EMPTY_LIST;
			}
				
				List<Object> r = new ArrayList<Object>(1);
				r.add(eo);
				return r;
	
	}

	@Override
	public void close() {
		this.query = null;
		this.connection = null;
		this.resultsIt = null;
	}

	@Override
	public void cancel()  {
	}
	
	public static String getNameInSource(AbstractMetadataRecord c) {
		String name = c.getNameInSource();
		if (name == null || name.trim().isEmpty()) {
			return c.getName();
		}
		return name;
	}
	
//		    /**
//		     * Helper method to return the given array value as a collection.
//		     */
//		    @SuppressWarnings("unchecked")
//		    public static <T> List<T> toList(Object val, Class<T> elem, boolean mutable) {
//	        if (val == null)
//		            return null;
//		
//		        List<T> l;
//	        if (!elem.isPrimitive()) {
//		            // if an object array, use built-in list function
//		            l = Arrays.asList((T[]) val);
//	            if (mutable)
//		                l = new ArrayList<T>(l);
//		        } else {
//		            // convert to list of wrapper objects
//	            int length = Array.getLength(val);
//		            l = new ArrayList<T>(length);
//		            for (int i = 0; i < length; i++)
//		                l.add((T)Array.get(val, i));
//		        }
//		        return l;
//		    }

}
