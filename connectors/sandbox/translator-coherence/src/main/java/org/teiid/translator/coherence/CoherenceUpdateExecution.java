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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;

import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.resource.adapter.coherence.CoherenceConnection;
import org.teiid.resource.adapter.coherence.CoherenceFilterUtil;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.coherence.util.ObjectSourceMethodManager;
import org.teiid.translator.coherence.visitor.CoherenceVisitor;
import org.teiid.translator.coherence.visitor.DeleteVisitor;



/**
 * Please see the user's guide for a full description of capabilities, etc.
 * 
 * Description/Assumptions:
 * 
 */
public class CoherenceUpdateExecution implements UpdateExecution {
	protected CoherenceConnection connection;
	protected RuntimeMetadata metadata;
	protected ExecutionContext context;
	protected Command command;
	protected SourceCacheAdapter cacheTranslator;
	protected CoherenceVisitor visitor = null;
	protected int result;
	
	public CoherenceUpdateExecution(Command command,
			CoherenceConnection coherenceConnection,
			RuntimeMetadata metadata, ExecutionContext context, SourceCacheAdapter cacheTranslator) {
		this.connection = coherenceConnection;
		this.metadata = metadata;
		this.context = context;
		this.command = command;
		this.cacheTranslator = cacheTranslator;
		this.visitor = new CoherenceVisitor(metadata);
	}	
	
	/** execute generic update-class (either an update, delete, or insert)
	 * operation and returns a count of affected rows.  Since underlying
	 * Coherence operations (and this connector) can modify at most one cache
	 * object at a time, this will always return 1.  It will never
	 * actually return 0, because if an operation fails, a
	 * ConnectorException will be thrown instead.
	 * here for the sake of efficiency.
	 */
	@Override
	public void execute() throws TranslatorException {

		if (command instanceof Update) {
			executeUpdate();
		}
		else if (command instanceof Delete) {
			executeDelete();
		}
		else if (command instanceof Insert) {
			executeInsert();
		}
//		else {
//            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.incorrectCommandError"); //$NON-NLS-1$
//			throw new TranslatorException(msg);
//		}
	}
	
	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException,
			TranslatorException {
		return new int[] {1};
	}

	/**
	 * Private method to perform the inserting of an object into the cache
	 * @throws TranslatorException
	 */
	private void executeInsert()
			throws TranslatorException {

		Insert icommand = (Insert) command;		
		
		Table t = metadata.getTable(icommand.getTable().getMetadataObject().getFullName());
		
		// if the table has a foreign key, its must be a child (contained) object in the root
		if (t.getForeignKeys() != null && t.getForeignKeys().size() > 0) {
			this.addChildObject(t);
			return;
		}
		String pkColName = null;
		
		// process the top level object
		List<Column> pk = t.getPrimaryKey().getColumns();
		if (pk == null || pk.isEmpty()) {
            final String msg = CoherencePlugin.Util.getString("CoherenceUpdateExecution.noPrimaryKeyDefinedOnTable", new Object[] {t.getName()}); //$NON-NLS-1$
			throw new TranslatorException(msg);		
		}
		
		pkColName = visitor.getNameFromElement(pk.get(0));
		
		Object newObject = cacheTranslator.createObject(icommand.getColumns(), 
				((ExpressionValueSource)icommand.getValueSource()).getValues(), 
				this.visitor, 
				t);
		
		// get the key value to use to for adding to the cache
		Object keyvalue = ObjectSourceMethodManager.getValue("get" + pkColName, newObject);
	
		// add to cache
		try {
			this.connection.add(keyvalue, newObject);
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}			

		
	}
	
	private void addChildObject(Table t) throws TranslatorException {
		List<ForeignKey> fks = t.getForeignKeys();
		
		ForeignKey fk = fks.get(0);
		Table parentTable = fk.getParent();
		// the name of the method to obtain the collection is the nameInSource of the foreginKey
		String parentToChildMethod = fk.getNameInSource();
		if(parentToChildMethod == null) {
            final String msg = CoherencePlugin.Util.getString("CoherenceUpdateExecution.noNameInSourceForForeingKey", new Object[] {fk.getName()}); //$NON-NLS-1$
			throw new TranslatorException(msg);
		}		
		
		
		// there must only be 1 column in the primary key
		String parentColName = visitor.getNameFromElement(fk.getPrimaryKey().getColumns().get(0));
		
		Insert icommand = (Insert) command;	
		
		List<ColumnReference> insertElementList = icommand.getColumns();
		List<Expression> insertValueList = ((ExpressionValueSource)icommand.getValueSource()).getValues();
		if(insertElementList.size() != insertValueList.size()) {
			throw new TranslatorException("Error:  columns.size and values.size are not the same.");
		}

		ColumnReference insertElement;
		String[] nameOfElement = new String[insertElementList.size()];
		
		int parentValueLoc = -1;

		for (int i=0; i < insertElementList.size(); i++) {
			insertElement = insertElementList.get(i);
//			// call utility class to get NameInSource/Name of element
			nameOfElement[i]= visitor.getNameFromElement(insertElement.getMetadataObject());
			
			// match the parent column to the colum in the insert statement
				if (nameOfElement[i].equalsIgnoreCase(parentColName)) {
					parentValueLoc = i;					
				}				
		}	
		
		if (parentColName != null && parentValueLoc == -1) {
	            final String msg = CoherencePlugin.Util.getString("CoherenceUpdateExecution.noColumnMatchedForeignColumn", new Object[] {t.getName(), parentColName}); //$NON-NLS-1$
				throw new TranslatorException(msg);
				
		}
		
		// get the parent key and find the root object
		Object parentValue = insertValueList.get(parentValueLoc);
		Object val;
		if(parentValue instanceof Literal) {
			Literal literalValue = (Literal)parentValue;
			val = literalValue.getValue();
		} else {
			val = parentValue;
		}
		
		Object parentObject = null;
		// get the parent object from the cache
		try {
			List<Object> result = this.connection.get(  CoherenceFilterUtil.createCompareFilter(parentColName, val, Operator.EQ, val.getClass())   );
			//		visitor.createFilter(parentColName + " = " + val));
			if (result == null || result.isEmpty()) {
	            final String msg = CoherencePlugin.Util.getString("CoherenceUpdateExecution.noobjectfound", new Object[] {parentTable.getName(), parentColName, val}); //$NON-NLS-1$
				throw new TranslatorException(msg);
			}
			parentObject = result.get(0);
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}			

		// create and load the child object data
		Object newChildObject = cacheTranslator.createObject(insertElementList, 
				insertValueList, 
				this.visitor, 
				t);
		
		///--- questions
		/// --- how to not process - setvalue for parent column
		/// --- need to get the key value off the object of the parent
		
		// get the key value to use to for adding to the cache
		Object parentContainer = ObjectSourceMethodManager.getValue("get" + parentToChildMethod, parentObject);
		if (parentContainer == null) {
	        final String msg = CoherencePlugin.Util.getString("CoherenceUpdateExecution.noParentContainerObjectFound", new Object[] {parentTable.getName(), parentToChildMethod}); //$NON-NLS-1$
			throw new TranslatorException(msg);	

		}
		
		if (parentContainer.getClass().isArray() ) {
			
		} else if  (parentContainer instanceof Collection) {
			Collection c = (Collection) parentContainer;
			c.add(newChildObject);
		} else if  ( parentContainer instanceof Map) {
			
			Map m = (Map) parentContainer;
			
			m.put(1, newChildObject);
		}

		try {
			this.connection.update(parentValue, parentObject);
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}			
		
	}

	private void executeDelete()
			throws TranslatorException {
		
		DeleteVisitor visitor = new DeleteVisitor(metadata);
		
    	visitor.visitNode((Delete) command);
    	
        if(visitor.getException() != null) { 
            throw visitor.getException();
        }

        if (visitor.getKeys() == null || visitor.getKeys().isEmpty()) {
            final String msg = CoherencePlugin.Util.getString("CoherenceUpdateExecution.objectNotDeleted", new Object[] {visitor.getTableName()}); //$NON-NLS-1$
			throw new TranslatorException(msg);	
        }

        for (java.util.Iterator it=visitor.getKeys().iterator(); it.hasNext();) {
        	Object key = it.next();
			try {
				this.connection.remove(key);
			} catch (ResourceException e) {
				throw new TranslatorException(e);
			}	
        }

	}

	// Private method to actually do an update operation.  
	private void executeUpdate()
			throws TranslatorException {
		Update ucommand = (Update) command;	
		
		Table t = metadata.getTable(ucommand.getTable().getMetadataObject().getFullName());
//		List<ForeignKey> fks = t.getForeignKeys();
		
		// if the table has a foreign key, its must be a child (contained) object in the root
		if (t.getForeignKeys() != null && t.getForeignKeys().size() > 0) {
			 updateChildObject(t);
			 return;
		}

	}
	
	private void updateChildObject(Table t) throws TranslatorException {
		List<ForeignKey> fks = t.getForeignKeys();
		
		ForeignKey fk = fks.get(0);
		Table parentTable = fk.getParent();
		// the name of the method to obtain the collection is the nameInSource of the foreginKey
		String parentToChildMethod = fk.getNameInSource();
		if(parentToChildMethod == null) {
            final String msg = CoherencePlugin.Util.getString("CoherenceUpdateExecution.noNameInSourceForForeingKey", new Object[] {fk.getName()}); //$NON-NLS-1$
			throw new TranslatorException(msg);
		}		
		
		// there must only be 1 column in the primary key
		String parentColName = visitor.getNameFromElement(fk.getPrimaryKey().getColumns().get(0));
		
		List<SetClause> updateList = ((Update)command).getChanges();
		Condition criteria = ((Update)command).getWhere();
		
		ColumnReference leftElement;
		Expression rightExpr;
		String nameLeftElement;
		Object valueRightExpr;
		// iterate through the supplied list of updates (each of
		// which is an ICompareCriteria with an IElement on the left
		// side and an IExpression on the right, per the Connector
		// API).
		for (int i=0; i < updateList.size(); i++) {
			SetClause setClause = updateList.get(i);
			// trust that connector API is right and left side
			// will always be an IElement
			leftElement = setClause.getSymbol();
			// call utility method to get NameInSource/Name for element
			nameLeftElement = visitor.getNameFromElement(leftElement.getMetadataObject());
			// get right expression - if it is not a literal we
			// can't handle that so throw an exception
			rightExpr = setClause.getValue();
//			if (!(rightExpr instanceof Literal)) { 
//	            final String msg = CoherencePlugin.Util.getString("LDAPUpdateExecution.valueNotLiteralError",nameLeftElement); //$NON-NLS-1$
//				throw new TranslatorException(msg);
//		}
			valueRightExpr = ((Literal)rightExpr).getValue();
			// add in the modification as a replacement - meaning
			// any existing value(s) for this attribute will
			// be replaced by the new value.  If the attribute
			// didn't exist, it will automatically be created
			// TODO - since null is a valid attribute
			// value, we don't do any special handling of it right
			// now.  But maybe null should mean to delete an
			// attribute?
		}		


	}
	

	// cancel here by closing the copy of the ldap context (if it was
	// initialized, which is only true if execute() was previously called)
	// calling close on already closed context is safe per
	// javax.naming.Context javadoc so we won't worry about this also
	// happening in our close method
	public void cancel() throws TranslatorException {
		close();
	}

	// close here by closing the copy of the ldap context (if it was
	// initialized, which is only true if execute() was previously called)
	// calling close on already closed context is safe per
	// javax.naming.Context javadoc so we won't worry about this also
	// happening in our close method
	public void close() {

	}


}
	



				
				
				
				
				
							
