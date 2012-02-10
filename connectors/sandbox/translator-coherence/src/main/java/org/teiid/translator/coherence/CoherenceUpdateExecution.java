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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.resource.ResourceException;

import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.language.Comparison.Operator;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.resource.adapter.coherence.CoherenceConnection;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.coherence.visitor.CoherenceVisitor;
import org.teiid.translator.coherence.visitor.DeleteVisitor;

import com.tangosol.coherence.component.util.Collections;
import com.tangosol.coherence.component.util.Iterator;



/**
 * Please see the user's guide for a full description of capabilties, etc.
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

		RuntimeMetadataImpl impl = (RuntimeMetadataImpl) metadata;
		
		Table t = metadata.getTable(icommand.getTable().getMetadataObject().getFullName());
		List<ForeignKey> fks = t.getForeignKeys();
		
		String parentToChildMethod = null;
		String pkColName = null;
		
		// if a foreign key exist, then what's being updated is contained in its parent object.
		// gather all the foreign key related info that will be needed
		String parentColName = null;
		Table parentTable = null;
		// if a foreign key is defined, then this is a child object being processed
		if (fks.size() > 0) {
			ForeignKey fk = fks.get(0);
			parentTable = fk.getParent();
			// the name of the method to obtain the collection is the nameInSource of the foreginKey
			parentToChildMethod = fk.getNameInSource();
			
			// there must only be 1 column in the primary key
			parentColName = visitor.getNameFromElement(fk.getPrimaryKey().getColumns().get(0));
					
		} else {
			// process the top level object
			List<Column> pk = t.getPrimaryKey().getColumns();
			if (pk == null || pk.isEmpty()) {
	            final String msg = CoherencePlugin.Util.getString("CoherenceUpdateExecution.noPrimaryKeyDefinedOnTable", new Object[] {t.getName()}); //$NON-NLS-1$
				throw new TranslatorException(msg);		
			}
			
			pkColName = visitor.getNameFromElement(pk.get(0));
		
		}
		
		
		List<ColumnReference> insertElementList = icommand.getColumns();
		List<Expression> insertValueList = ((ExpressionValueSource)icommand.getValueSource()).getValues();
		if(insertElementList.size() != insertValueList.size()) {
			throw new TranslatorException("Error:  columns.size and values.size are not the same.");
		}
		
		Object parentObject = null;
		Object parentContainer = null;
		ColumnReference insertElement;
		String[] nameOfElement = new String[insertElementList.size()];
		
		int parentValueLoc = -1;

		for (int i=0; i < insertElementList.size(); i++) {
			insertElement = insertElementList.get(i);
//			// call utility class to get NameInSource/Name of element
			nameOfElement[i]= visitor.getNameFromElement(insertElement.getMetadataObject());
			
			if (parentColName != null) {
				if (nameOfElement[i].equalsIgnoreCase(parentColName)) {
					parentValueLoc = i;					
				}
			}
				
		}		
		
		// this loop will
		// 	-  get the nameInSource for each column that will be used to set the value
		//  -  (and primarily), determine the location of the column that corresponds to the foreign key
		//		the value for the foreign key will be needed to find the parent
		if (parentColName != null) {
			if (parentValueLoc == -1) {
	            final String msg = CoherencePlugin.Util.getString("CoherenceUpdateExecution.noColumnMatchedForeignColumn", new Object[] {t.getName(), parentColName}); //$NON-NLS-1$
				throw new TranslatorException(msg);
				
			}
			Object parentValue = insertValueList.get(parentValueLoc);
			Object val;
			if(parentValue instanceof Literal) {
				Literal literalValue = (Literal)parentValue;
				val = literalValue.getValue();
				//.toString();
			} else {
				val = parentValue;
				//.toString();
			}
			
			// get the parent object from the cache
			try {
				List<Object> result = this.connection.get(visitor.createFilter(parentColName + " = " + val));
				if (result == null || result.isEmpty()) {
		            final String msg = CoherencePlugin.Util.getString("CoherenceUpdateExecution.noobjectfound", new Object[] {parentTable.getName(), parentColName, val}); //$NON-NLS-1$
					throw new TranslatorException(msg);
				}
				parentObject = result.get(0);
			} catch (ResourceException e) {
				throw new TranslatorException(e);
			}			
			
			List<String> token = new ArrayList<String>(1);
			token.add(parentToChildMethod);
			 
			// call to get the Object on the parent, which will be a container of some sort, for which the new object will be added
			parentContainer = cacheTranslator.getValue(visitor.getTableName(), token, Collections.class, parentObject, 0);

			
		} 
		// create the new object that will either be added as a top level object or added to the parent container
		String tableName =  this.visitor.getNameFromTable(t);
		Object newObject = cacheTranslator.createObjectFromMetadata(tableName);		

		Object keyvalue = null;
		for (int i=0; i < insertElementList.size(); i++) {
			insertElement = insertElementList.get(i);
			
			Object value = insertValueList.get(i);
			Object val;
			if(value instanceof Literal) {
				Literal literalValue = (Literal)value;
				val = literalValue.getValue();
				//.toString();
				if(null != val && val instanceof String) {
					//!val.isEmpty()) {
					val = this.stripQutes((String) val);
				}
			} else {
				val = value;
				//.toString();
			}

			if (parentColName == null && nameOfElement[i].equalsIgnoreCase(pkColName)) {
				keyvalue = val;
			}
			
		    this.cacheTranslator.setValue(tableName, nameOfElement[i], newObject, val, insertElement.getType()); 
		}

		if (parentColName == null) {
			// add top level object
			try {
				this.connection.add(keyvalue, newObject);
			} catch (ResourceException e) {
				throw new TranslatorException(e);
			}			
		} else {
			// TODO:  add logic to add Map and Array's
			((Collection) parentContainer).add(newObject);

		}
		
	}


	private void executeDelete()
			throws TranslatorException {
		
		DeleteVisitor visitor = new DeleteVisitor(metadata);
		
    	visitor.visitNode((Delete) command);
    	
        if(visitor.getException() != null) { 
            throw visitor.getException();
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

	// Private method to actually do an update operation.  Per JNDI doc at
	// http://java.sun.com/products/jndi/tutorial/ldap/models/operations.html, 
	// the JNDI method to use to update an entry to LDAP is one of the
	// DirContext.modifyAttributes() methods that takes ModificationItem[]
	// as a parameter, so that is what is used here.
	// Note that this method does not allow for changing of the DN - to
	// implement that we would need to use Context.rename().  Since right
	// now we only call modifyAttributes(), and don't check for the DN
	// in the list of updates, we will attempt to update the DN using
	// modifyAttributes(), and let the LDAP server fail the request (and
	// send us the explanation for the failure, which is returned in
	// a ConnectorException)
	//
	// The update criteria must include only an equals comparison
	// on the "DN" column ("WHERE DN='cn=John Doe,ou=people,dc=company,dc=com'") 
	private void executeUpdate()
			throws TranslatorException {
//
//		List<SetClause> updateList = ((Update)command).getChanges();
//		Condition criteria = ((Update)command).getWhere();
//
//		// since we have the exact same processing rules for criteria
//		// for updates and deletes, we use a common private method to do this.
//		// note that this private method will throw a ConnectorException
//		// for illegal criteria, which we deliberately don't catch
//		// so it gets passed on as is.
//		String distinguishedName = getDNFromCriteria(criteria);
//		
//
//		// this will be the list of modifications to attempt.  Since
//		// we currently blindly try all the updates the query
//		// specifies, right now this is the same size as the updateList.
//		// When we start filtering out DN changes (which would need to
//		// be performed separately using Context.rename()), we will
//		// need to account for this in determining this list size. 
//		ModificationItem[] updateMods = new ModificationItem[updateList.size()];
//		ColumnReference leftElement;
//		Expression rightExpr;
//		String nameLeftElement;
//		Object valueRightExpr;
//		// iterate through the supplied list of updates (each of
//		// which is an ICompareCriteria with an IElement on the left
//		// side and an IExpression on the right, per the Connector
//		// API).
//		for (int i=0; i < updateList.size(); i++) {
//			SetClause setClause = updateList.get(i);
//			// trust that connector API is right and left side
//			// will always be an IElement
//			leftElement = setClause.getSymbol();
//			// call utility method to get NameInSource/Name for element
//			nameLeftElement = getNameFromElement(leftElement);
//			// get right expression - if it is not a literal we
//			// can't handle that so throw an exception
//			rightExpr = setClause.getValue();
//			if (!(rightExpr instanceof Literal)) { 
//	            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.valueNotLiteralError",nameLeftElement); //$NON-NLS-1$
//				throw new TranslatorException(msg);
//		}
//			valueRightExpr = ((Literal)rightExpr).getValue();
//			// add in the modification as a replacement - meaning
//			// any existing value(s) for this attribute will
//			// be replaced by the new value.  If the attribute
//			// didn't exist, it will automatically be created
//			// TODO - since null is a valid attribute
//			// value, we don't do any special handling of it right
//			// now.  But maybe null should mean to delete an
//			// attribute?
//		        updateMods[i] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute(nameLeftElement, valueRightExpr));
//		}
//		// just try to update an LDAP entry using the DN and
//		// attributes specified in the UPDATE operation.  If it isn't
//		// legal, we'll get a NamingException back, whose explanation
//		// we'll return in a ConnectorException
//		try {
//			ldapCtx.modifyAttributes(distinguishedName, updateMods);
//		} catch (NamingException ne) {
//            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.updateFailed",distinguishedName,ne.getExplanation()); //$NON-NLS-1$
//			throw new TranslatorException(msg);
//		// don't remember why I added this generic catch of Exception,
//		// but it does no harm...
//		} catch (Exception e) {
//            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.updateFailedUnexpected",distinguishedName); //$NON-NLS-1$
//			throw new TranslatorException(e, msg);
//		}
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
//		try {
//			if(ldapCtx != null) {
//				ldapCtx.close();
//			}
//		} catch (NamingException ne) {
//            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.closeContextError",ne.getExplanation()); //$NON-NLS-1$
//            LogManager.logWarning(LogConstants.CTX_CONNECTOR,msg);
//		}
	}
	
	private String stripQutes(String id) {
		if((id.startsWith("'") && id.endsWith("'"))) {
			id = id.substring(1,id.length()-1);
		} else if ((id.startsWith("\"") && id.endsWith("\""))) {
			id = id.substring(1,id.length()-1);
		}
		return id;
	}

}
	



				
				
				
				
				
							
