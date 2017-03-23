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
import org.teiid.language.Command;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.ForeignKey;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.util.ObjectUtil;

/**
 * Execution of the SELECT Command
 */
public class ObjectExecution extends ObjectBaseExecution implements ResultSetExecution {
	public enum  OBJECT_TYPE {
		COLLECTION,
		ARRAY,
		MAP,
		VALUE
	}

	
	class DepthNode {
		
		protected List<String> nodes = null;
		protected String depthNodes = null;
		protected int nodeSize = 0;
		protected int columnLoc = 0;
		
		// values that change during processing
		protected int nodePosition=0;
		protected Object value = null;  
		protected OBJECT_TYPE ot;
		
		private DepthNode() {
			
		}
		DepthNode(String name, int colLocation) {
			this();
			nodes = StringUtil.split(name, ".");
			nodeSize = nodes.size();
			depthNodes = name.substring(0, name.lastIndexOf("."));
			columnLoc = colLocation;
	
		}
		
		int getColumnLocation() {
			return columnLoc;
		}
		
		String getNodeName(int position) {
			return nodes.get(position);
		}
		
		String getCurrentNodeName() {
			return nodes.get(nodePosition);
		}
		
		int getNumberOfNodes() {
			return nodeSize;
		}
		
		String getDepthNodes() {
			return depthNodes;
		}
		
		String getDepthMethodName() {
			return nodes.get(nodeSize -1);
		}
		
		boolean hasNext() {
			return (nodePosition < (nodeSize -1) ?true:false);
		}
		
		boolean isLast() {
			return ( nodePosition == (nodeSize - 1) ? true:false);
		}
		
		void incrementPosition() {
			++nodePosition;
		}
		
		Object getValue() {
			return value;
		}
		
		boolean isCollection() {
			return (ot == OBJECT_TYPE.COLLECTION);
		}
		
		boolean isMap() {
			return (ot == OBJECT_TYPE.MAP);
		}

		boolean isArray() {
			return (ot == OBJECT_TYPE.ARRAY);
		}
		
		boolean isObjectValue() { 
			return (ot == OBJECT_TYPE.VALUE);
		}
		
		/**
		 * Called to reset pointer to what it was before 
		 * being processed.  This is so the this node can be reused
		 * for each object in the object result set for this column.
		 */
		void reset() {
			nodePosition=0;
			value = null;
			ot=null;
		}
		
		void setValue(Object v) {
			value = v;
			if (Collection.class.isAssignableFrom(v.getClass())) {
				ot = OBJECT_TYPE.COLLECTION;	
			} else if (Map.class.isAssignableFrom(v.getClass())) {
				ot = OBJECT_TYPE.MAP;
			} else if (v.getClass().isArray()) {
				ot = OBJECT_TYPE.ARRAY;
			} else {
				ot = OBJECT_TYPE.VALUE;
			}
		}

		@Override
		protected Object clone() {
			DepthNode dn = new DepthNode();
			dn.nodePosition = this.nodePosition;
			dn.nodes = this.nodes;
			dn.depthNodes = this.depthNodes;
			dn.nodeSize = this.nodeSize;
			dn.columnLoc = this.columnLoc;
			return dn;
		}
	}
	
	protected Command query;
	private Object[] colObjects;
	private ScriptContext sc = new SimpleScriptContext();
	private ObjectScriptEngine scriptEngine;
	private Iterator<Object> objResultsItr = null;
	private Iterator<Object> cacheResultsIt = null;
	private ObjectVisitor visitor;
	private int depth = 0; // the bottom depth to go, not all depths may retrieve data/
	private int colSize = 0;

	public ObjectExecution(Command command, 
			ObjectExecutionFactory factory, ObjectConnection connection, ExecutionContext executionContext) throws TranslatorException {
		super(connection, executionContext, factory);
		this.query = command;
		this.scriptEngine = connection.getClassRegistry().getReadScriptEngine();
		
		visitor = this.createVisitor();
		
		visitor.visitNode(query);
		
		// throw the 1st exeception in the list
		if (visitor.getExceptions() != null && !visitor.getExceptions().isEmpty()) {
			throw visitor.getExceptions().get(0);
		}

		List<DerivedColumn> cols = visitor.getProjectedColumns();

		colSize = cols.size();

		ForeignKey fk = visitor.getForeignKey();
		
		colObjects = new Object[colSize];
		
		int col = 0;
		for (DerivedColumn dc : cols) {
			ColumnReference cr = (ColumnReference) dc.getExpression();
			
			String nis = ObjectUtil.getRecordName(cr.getMetadataObject());

			if (nis.equalsIgnoreCase("this")) { //$NON-NLS-1$
					// the object in cache is being requested
					colObjects[col] = null;					
					
					// nis with a period indicates an internal class to the root
			} else if (nis.indexOf(".") > 0)  {
					if (fk == null) {
						throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21003, new Object[] { ((Select)query).getFrom()}));
					}
					DepthNode dn = new DepthNode(nis, col);
					colObjects[col] = dn;
					 						
					int n =dn.getNumberOfNodes();

					if (n > depth) depth = n;
					
			} else {
				
				try {
					colObjects[col] = getCompiledNode(nis);
				} catch (ScriptException e) {
					throw new TranslatorException(e);
				}
			}
			col++;
		}
	}

	@Override
	public void execute() throws TranslatorException {

		if (visitor.getPrimaryTable() != null) {
			connection.getDDLHandler().setStagingTarget(true);
		}
		try {
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.TRACE)) {
				LogManager.logTrace(LogConstants.CTX_CONNECTOR,
						"ObjectExecution command:", query.toString(), "using connection:", connection.getClass().getName()); //$NON-NLS-1$ //$NON-NLS-2$
			}
		    
			// column NIS for a column will be used to query the cache
		    List<Object> objResults = connection.getSearchType().performSearch(visitor, executionContext); 
		    		//env.search(visitor, connection, executionContext);
		    
			if (objResults == null) {
				objResults = Collections.emptyList();
			}
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,
					"ObjectExecution: number of returned objects is :", objResults.size() ); //$NON-NLS-1$
			
			this.objResultsItr = objResults.iterator();
		} catch (TranslatorException te) {
			throw te;
		} catch (RuntimeException re) {
			// Cache containers, like JDG, can throw runtime exception when there are problems accessing the data in the cache, even though the 
			// container and cache are available.  So translator needs indicate there was an issue connecting to the cache source.
			this.connection.forceCleanUp();
			throw new TranslatorException(ObjectPlugin.Util.gs(ObjectPlugin.Event.TEIID21019));

		} finally {
			connection.getDDLHandler().setStagingTarget(false);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<?> next() throws TranslatorException,
			DataNotAvailableException {
		if  (cacheResultsIt != null && cacheResultsIt.hasNext()) {
		
			// return the next row in the cache 
			return (List<Object>) cacheResultsIt.next();
		} 
		
		String fkeyColNIS = null;
		cacheResultsIt = null;
		if (depth > 0 ) {

			fkeyColNIS = visitor.getForeignKey().getNameInSource();
		}
		
		// process the next object in the search result set
		while (objResultsItr.hasNext()) {
			
			final Object o = objResultsItr.next();
			
			if (depth > 0) {
				
				Method rootClassReadMethod = ClassRegistry.findMethod(this.getClassRegistry().getReadClassMethods(this.connection.getCacheClassType().getName()), fkeyColNIS, this.connection.getCacheClassType().getName());

				Object parentValue = null;
				try {
					parentValue = ClassRegistry.executeGetMethod(rootClassReadMethod, o);
				} catch (Exception e1) {
					throw new TranslatorException(e1);
				}
				
				final List<Object> rows = processNode(o, parentValue);

				if (rows != null && rows.size() > 0) {
				
					if (rows.size() < 2) return (List<Object>) rows.get(0);
					
					cacheResultsIt = rows.iterator();
					if (cacheResultsIt != null && cacheResultsIt.hasNext()) {
						// return the next row in the cache 
						return (List<Object>) cacheResultsIt.next();
					} 
				}
			} else {
				final List<Object> r = new ArrayList<Object>(colSize);
				addColumnData(r, o, null);
				
				return r;
			}

		}
		return null;
	}
	
	private void addColumnData(List<Object> r, Object parent, Object child) throws TranslatorException {
		

		for (int i = 0; i < colSize; i++) {
			if (colObjects[i] == null) {
				r.add( (child != null ? child : parent));
				continue;
			}
			try {
				
				if (colObjects[i] instanceof CompiledScript) {
					sc.setAttribute(ClassRegistry.OBJECT_NAME, parent, ScriptContext.ENGINE_SCOPE);	
					CompiledScript cs = (CompiledScript) colObjects[i];
					r.add(cs.eval(sc));
				} else {
					sc.setAttribute(ClassRegistry.OBJECT_NAME, child, ScriptContext.ENGINE_SCOPE);	
					DepthNode dn = (DepthNode) colObjects[i];	
					dn.reset();
					CompiledScript cs = getCompiledNode(dn.getDepthMethodName());
					
					r.add(cs.eval(sc));

				}				

			} catch (ScriptException e) {
				throw new TranslatorException(e);
			}
		}	

	}
	
//	// the recursive logic will traverse to the bottom depth and work its way back up
//	// expanding the the number of rows
//	@SuppressWarnings("rawtypes")
//	private List<Object> processDepth(List<Object> mainRow, Map<String, DepthNode> loadedDepths) throws TranslatorException {
//		
//		// assumptions:
//		// -  only 1 1-to-many relationship can be specified per query, so if more than one column maps to a collection, 
//		//		its assumed they all map to the same collection
//		
//		Object[] cols = new Object[colSize];
//
//		int rows = 0;
//		if (this.depth > 0) {
//			for (Object r: mainRow) {
//				if (r instanceof DepthNode) {
//					DepthNode dn = (DepthNode) r;
//					dn.incrementPosition();
//					if (dn.isLast() && dn.isObjectValue()) {
//						Object o = evaluate(dn);
//						dn.setValue(o);
//						cols[dn.getColumnLocation()] = o;
//					} else {
//						List<Object> newrows = processNode(dn);
//						dn.setValue(newrows);
//						cols[dn.getColumnLocation()] = newrows;
//						rows = newrows.size();
//					}
//				}
//			}
//		}
//		
//		// contains the rows to be returned
//		List<Object> results = new ArrayList<Object>();
//
//		for (int r=0; r < rows; r++) {
//			
//			List<Object> row = new ArrayList<Object>(colSize);
//
//			for (int c = 0; c < colSize; c++) {
//				Object o = mainRow.get(c);
//				if (cols[c] == null) {
//					row.add(o);
//				} else {
//					if (cols[c] instanceof List) {
//						List l = (List) cols[c];
//						row.add(l.get(r));
//					} else {
//						row.add(cols[c]);				
////						CompiledScript cs = getCompiledNode(depthNode.getCurrentNodeName());
//
//					}
//					
//				}
//					
//			}
//			
//			results.add(row);
//		}
//			
//		return results;	
//
//	}
//	
	@SuppressWarnings("rawtypes")
	private List<Object> processNode(Object parent, Object value) throws TranslatorException {
		// contains the rows to be returned
		List<Object> results = new ArrayList<Object>();

			//	Collections.emptyList();
		if (value == null) return results;
		
//		Object obj = depthNode.getValue();
//		if (obj == null) return rows;
		
		try {
			
			if (Collection.class.isAssignableFrom(value.getClass())) {
				Collection c = (Collection) value;
								
				for (Iterator it=c.iterator(); it.hasNext();) {
					Object o = it.next();
					
					final List<Object> r = new ArrayList<Object>(colSize);

					if (o == null) {
						r.add(null);
						continue;
					}
					
					addColumnData(r, parent, o);
					
					results.add(r);

				}
	
			} else if (Map.class.isAssignableFrom(value.getClass())) {
				Map m = (Map) value;
				
				Iterator it = m.values().iterator();
				while(it.hasNext()) {
					Object o = it.next();
					
					final List<Object> r = new ArrayList<Object>(colSize);
					
					if (o == null) {
						r.add(null);
						continue;
					}
					
					addColumnData(r, parent, o);
					results.add(r);

				}
			} else if (value.getClass().isArray()) {
				Object[] a = (Object[]) value;
								
				for (int i = 0; i < a.length; i++) {
					final List<Object> r = new ArrayList<Object>(colSize);
					
					if (a[i] == null) {
						r.add(null);
						continue;
					}
					
					addColumnData(r, parent, a[i]);
					results.add(r);

				}

			} else {
				final List<Object> r = new ArrayList<Object>(colSize);		
				addColumnData(r, parent, value);
				results.add(r);
			}

		} catch (TranslatorException te) {
			throw te;
		}
		return results;
	}
//	
//	@SuppressWarnings("rawtypes")
//	private List<Object> processNode(DepthNode depthNode) throws TranslatorException {
//		List<Object> rows = new ArrayList<Object>();
//			//	Collections.emptyList();
//		
//		Object obj = depthNode.getValue();
//		if (obj == null) return rows;
//		
//		try {
//			
//			if (depthNode.isCollection()) {
//				Collection c = (Collection) obj;
//				
//				CompiledScript cs = getCompiledNode(depthNode.getCurrentNodeName());
//								
//				for (Iterator it=c.iterator(); it.hasNext();) {
//					Object o = it.next();
//
//					if (o == null) {
//						rows.add(null);
//						continue;
//					}
//
//					sc.setAttribute(ClassRegistry.OBJECT_NAME, o, ScriptContext.ENGINE_SCOPE);
//
//					Object v = cs.eval(sc);
//					
//					DepthNode ndn;
//					ndn = (DepthNode) depthNode.clone();
//
//					ndn.setValue(v);
//					
//					if (ndn.hasNext()) {
//						ndn.incrementPosition();
//						
//						List<Object> values = processNode(ndn);
//						rows.add(values);
//					} else {
//						rows.add(v);
//					}
//				}
//				
//			} else if (depthNode.isMap()) {
//				Map m = (Map) obj;
//				
//				CompiledScript cs = getCompiledNode(depthNode.getCurrentNodeName());
//				depthNode.incrementPosition();
//				Iterator it = m.values().iterator();
//				while(it.hasNext()) {
//					Object o = it.next();
//					if (o == null) {
//						rows.add(null);
//						continue;
//					}
//					sc.setAttribute(ClassRegistry.OBJECT_NAME, o, ScriptContext.ENGINE_SCOPE);
//
//					Object v = cs.eval(sc);
//					
//					DepthNode ndn;
//					ndn = (DepthNode) depthNode.clone();
//					ndn.setValue(v);
//					ndn.incrementPosition();
//					
//					List<Object> values = processNode(ndn);
//					rows.add(values);
//
//				}
//				
//			} else if (depthNode.isArray()) {
//				Object[] a = (Object[]) obj;
//								
//				CompiledScript cs = getCompiledNode(depthNode.getCurrentNodeName());
//				depthNode.incrementPosition();
//				
//				for (int i = 0; i < a.length; i++) {
//
//					if (a[i] == null) {
//						rows.add(null);
//						continue;
//					}
//					sc.setAttribute(ClassRegistry.OBJECT_NAME, a[i], ScriptContext.ENGINE_SCOPE);
//
//					Object v = cs.eval(sc);
//					
//					DepthNode ndn;
//					ndn = (DepthNode) depthNode.clone();
//					ndn.setValue(v);
//					ndn.incrementPosition();
//					
//					List<Object> values = processNode(ndn);
//					rows.add(values);
//
//				}
//				// 
//			} else {
//				sc.setAttribute(ClassRegistry.OBJECT_NAME, depthNode.getValue(), ScriptContext.ENGINE_SCOPE);
//				Object v = getCompiledNode(depthNode.getCurrentNodeName()).eval(sc);
//				depthNode.incrementPosition();
//				rows.add(v);
//				return rows;
//			}
//
//		} catch (TranslatorException te) {
//			throw te;
//		} catch (ScriptException e) {
//			throw new TranslatorException(e);
//		}
//		return rows;
//	}
	
//	private Object evaluate(DepthNode depthNode) throws TranslatorException {
//		sc.setAttribute(ClassRegistry.OBJECT_NAME, depthNode.getValue(), ScriptContext.ENGINE_SCOPE);
//
//		Object v;
//		try {
//			v = getCompiledNode(depthNode.getCurrentNodeName()).eval(sc);
//		} catch (ScriptException e) {
//			throw new TranslatorException(e);
//		}
//		return v;
//
//	}
	
	@Override
	public void close() {
		super.close();
		this.query = null;
		this.colObjects = null;
		this.scriptEngine = null;
		this.sc = null;
	
		this.cacheResultsIt = null;
		
		if (visitor != null) {
			this.visitor.cleanUp();
		}
		this.visitor = null;
		
	}

	
	protected ObjectVisitor createVisitor() {
        return new ObjectVisitor();
    }
	
	
	
	private CompiledScript getCompiledNode(String nodeName) throws ScriptException {
		return scriptEngine.compile(ClassRegistry.OBJECT_NAME + "." + nodeName);
	}
	
	public ObjectExecutionFactory getFactory() {
		return this.env;
	}
	
	public Command getCommand() {
		return this.query;
	}
	
}
