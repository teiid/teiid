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
package com.metamatrix.connector.ldap;

import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.ICriteria;
import org.teiid.connector.language.IDelete;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IInsert;
import org.teiid.connector.language.IInsertExpressionValueSource;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.ISetClause;
import org.teiid.connector.language.IUpdate;
import org.teiid.connector.language.ICompareCriteria.Operator;
import org.teiid.connector.metadata.runtime.MetadataObject;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;


/**
 * Please see the user's guide for a full description of capabilties, etc.
 * 
 * Description/Assumptions:
 * 1. Table's name in source defines the base DN (or context) for the search.
 * Example: Table.NameInSource=ou=people,dc=gene,dc=com
 * 2. Column's name in source defines the LDAP attribute name.
 * [Default] If no name in source is defined, then we attempt to use the column name
 * as the LDAP attribute name.
 * 3. Since all of the underlying LDAP methods for adding/deleting/updating
 * require specification of the LDAP distinguished name (DN) to change, for all
 * corresponding MetaMatrix operations the DN must be specified (as the sole
 * item in the WHERE clause for UPDATE and DELETE operations, and in the list
 * of attributes to assign values in an INSERT operation * Responsible for update/insert/delete operations against LDAP
 */
public class LDAPUpdateExecution extends BasicExecution implements UpdateExecution {
	private ConnectorLogger logger;
	private RuntimeMetadata rm;
	private InitialLdapContext initialLdapContext;
	private LdapContext ldapCtx;
	private ICommand command;

	public LDAPUpdateExecution(ICommand command, ExecutionContext ctx,
			RuntimeMetadata rm, ConnectorLogger logger,
			InitialLdapContext ldapCtx) throws ConnectorException {
		this.rm = rm;
		this.logger = logger;
		this.initialLdapContext = ldapCtx;
		this.command = command;
	}
	
	/** execute generic update-class (either an update, delete, or insert)
	 * operation and returns a count of affected rows.  Since underlying
	 * LDAP operations (and this connector) can modify at most one LDAP
	 * leaf context at a time, this will always return 1.  It will never
	 * actually return 0, because if an operation fails, a
	 * ConnectorException will be thrown instead.
	 * Note that really it should return 0 if a delete is performed on
	 * an entry that doesn't exist (but whose parent does exist), but
	 * since the underlying LDAP operation will return success for such a
	 * delete, we just blindly return 1.  To return 0 would mean performing
	 * a search for the entry first before deleting it (to confirm that it
	 * did exist prior to the delete), so right now we sacrifice accuracy
	 * here for the sake of efficiency.
	 */
	@Override
	public void execute() throws ConnectorException {
		// first make a copy of the initial LDAP context we got from
		// the connection.  The actual update-class operation will use
		// this copy.  This will enable the close and cancel methods
		// to stop any LDAP operations we are making by calling the
		// close() method of the copy context, without closing our
		// real connection to the LDAP server
		try {
			ldapCtx = (LdapContext)this.initialLdapContext.lookup("");  //$NON-NLS-1$
		} catch (NamingException ne) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.createContextError",ne.getExplanation()); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}

		if (command instanceof IUpdate) {
			executeUpdate();
		}
		else if (command instanceof IDelete) {
			executeDelete();
		}
		else if (command instanceof IInsert) {
			executeInsert();
		}
		else {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.incorrectCommandError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
	}
	
	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException,
			ConnectorException {
		return new int[] {1};
	}


	// Private method to actually do an insert operation.  Per JNDI doc at
	// http://java.sun.com/products/jndi/tutorial/ldap/models/operations.html, JNDI method to add new entry to LDAP that does not contain a Java object is
	// DirContext.createSubContext(), so that is what is used here.
	//
	// The insert must include an element named "DN" (case insensitive)
	// which will be the fully qualified LDAP distinguished name of the 
	// entry to add.
	//
	// Also, while we make no effort to prevent insert operations that
	// break these rules, the underlying LDAP operation will fail (and
	// pass back an explanatory message, which we will return in a
	// ConnectorException, in the following cases:
	// -if the parent context for this entry does not exist in the directory
	// -if the insert does not specify values for all required attributes
	// of the class.  Since objectClass is required for all LDAP entries,
	// if it is not specified this condition will apply - and once it is
	// specified then all of the other required attributes for that
	// objectClass will of course also be required.
	//
	// Just as with the read support in the LDAPSyncQueryExecution class,
	// multi-value attributes are not supported by this implementation.
	// 
	// TODO - maybe automatically specify objectClass based off of
	// Name/NameInSource RESTRICT property settings, like with read support
	private void executeInsert()
			throws ConnectorException {

		List insertElementList = ((IInsert)command).getElements();
		List insertValueList = ((IInsertExpressionValueSource)((IInsert)command).getValueSource()).getValues();
		// create a new attribute list with case ignored in attribute
		// names
		Attributes insertAttrs = new BasicAttributes(true);
		Attribute insertAttr; // what we will use to populate the attribute list
		IElement insertElement;
		String nameInsertElement;
		Object insertValue;
		String distinguishedName = null;
		// The IInsert interface uses separate List objects for
		// the element names and values to be inserted, limiting
		// the potential for code reuse in reading them (since all
		// other interfaces use ICriteria-based mechanisms for such
		// input).
		for (int i=0; i < insertElementList.size(); i++) {
			insertElement = (IElement)insertElementList.get(i);
			// call utility class to get NameInSource/Name of element
			nameInsertElement = getNameFromElement(insertElement);
			// special handling for DN attribute - use it to set
			// distinguishedName value.
			if (nameInsertElement.toUpperCase().equals("DN")) {  //$NON-NLS-1$
				insertValue = ((ILiteral)insertValueList.get(i)).getValue();
				if (insertValue == null) { 
		            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.columnSourceNameDNNullError"); //$NON-NLS-1$
					throw new ConnectorException(msg);
				}
				if (!(insertValue instanceof java.lang.String)) {
		            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.columnSourceNameDNTypeError"); //$NON-NLS-1$
					throw new ConnectorException(msg);
				}
				distinguishedName = (String)insertValue;
			}
			// for other attributes specified in the insert command,
			// create a new 
			else {
				insertAttr = new BasicAttribute(nameInsertElement);
				insertValue = ((ILiteral)insertValueList.get(i)).getValue();
				insertAttr.add(insertValue);
				insertAttrs.put(insertAttr);
			}
		}
		// if the DN is not specified, we don't know enough to attempt
		// the LDAP add operation, so throw an exception
		if (distinguishedName == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.noInsertSourceNameDNError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		// just try to create a new LDAP entry using the DN and
		// attributes specified in the INSERT operation.  If it isn't
		// legal, we'll get a NamingException back, whose explanation
		// we'll return in a ConnectorException
		try {
			ldapCtx.createSubcontext(distinguishedName, insertAttrs);
		} catch (NamingException ne) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.insertFailed",distinguishedName,ne.getExplanation()); //$NON-NLS-1$
			throw new ConnectorException(msg);
		} catch (Exception e) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.insertFailedUnexpected",distinguishedName); //$NON-NLS-1$
			throw new ConnectorException(e, msg);
		}
	}

	// Private method to actually do a delete operation.  Per JNDI doc at
	// http://java.sun.com/products/jndi/tutorial/ldap/models/operations.html, 
	// a good JNDI method to delete an entry to LDAP is
	// DirContext.destroySubContext(), so that is what is used here.
	//
	// The delete criteria must include only an equals comparison
	// on the "DN" column ("WHERE DN='cn=John Doe,ou=people,dc=company,dc=com'") 
	// Note that the underlying LDAP operations here return successfully
	// even if the named entry doesn't exist (as long as its parent does
	// exist).
	private void executeDelete()
			throws ConnectorException {

		ICriteria criteria = ((IDelete)command).getCriteria();

		// since we have the exact same processing rules for criteria
		// for updates and deletes, we use a common private method to do this.
		// note that this private method will throw a ConnectorException
		// for illegal criteria, which we deliberately don't catch
		// so it gets passed on as is.
		String distinguishedName = getDNFromCriteria(criteria);

		// just try to delete an LDAP entry using the DN
		// specified in the DELETE operation.  If it isn't
		// legal, we'll get a NamingException back, whose explanation
		// we'll return in a ConnectorException
		try {
			ldapCtx.destroySubcontext(distinguishedName);
		} catch (NamingException ne) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.deleteFailed",distinguishedName,ne.getExplanation()); //$NON-NLS-1$
			throw new ConnectorException(msg);
		// don't remember why I added this generic catch of Exception,
		// but it does no harm...
		} catch (Exception e) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.deleteFailedUnexpected",distinguishedName); //$NON-NLS-1$
			throw new ConnectorException(e, msg);
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
			throws ConnectorException {

		List<ISetClause> updateList = ((IUpdate)command).getChanges().getClauses();
		ICriteria criteria = ((IUpdate)command).getCriteria();

		// since we have the exact same processing rules for criteria
		// for updates and deletes, we use a common private method to do this.
		// note that this private method will throw a ConnectorException
		// for illegal criteria, which we deliberately don't catch
		// so it gets passed on as is.
		String distinguishedName = getDNFromCriteria(criteria);
		

		// this will be the list of modifications to attempt.  Since
		// we currently blindly try all the updates the query
		// specifies, right now this is the same size as the updateList.
		// When we start filtering out DN changes (which would need to
		// be performed separately using Context.rename()), we will
		// need to account for this in determining this list size. 
		ModificationItem[] updateMods = new ModificationItem[updateList.size()];
		IElement leftElement;
		IExpression rightExpr;
		String nameLeftElement;
		Object valueRightExpr;
		// iterate through the supplied list of updates (each of
		// which is an ICompareCriteria with an IElement on the left
		// side and an IExpression on the right, per the Connector
		// API).
		for (int i=0; i < updateList.size(); i++) {
			ISetClause setClause = updateList.get(i);
			// trust that connector API is right and left side
			// will always be an IElement
			leftElement = setClause.getSymbol();
			// call utility method to get NameInSource/Name for element
			nameLeftElement = getNameFromElement(leftElement);
			// get right expression - if it is not a literal we
			// can't handle that so throw an exception
			rightExpr = setClause.getValue();
			if (!(rightExpr instanceof ILiteral)) { 
	            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.valueNotLiteralError",nameLeftElement); //$NON-NLS-1$
				throw new ConnectorException(msg);
		}
			valueRightExpr = ((ILiteral)rightExpr).getValue();
			// add in the modification as a replacement - meaning
			// any existing value(s) for this attribute will
			// be replaced by the new value.  If the attribute
			// didn't exist, it will automatically be created
			// TODO - since null is a valid attribute
			// value, we don't do any special handling of it right
			// now.  But maybe null should mean to delete an
			// attribute?
		        updateMods[i] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute(nameLeftElement, valueRightExpr));
		}
		// just try to update an LDAP entry using the DN and
		// attributes specified in the UPDATE operation.  If it isn't
		// legal, we'll get a NamingException back, whose explanation
		// we'll return in a ConnectorException
		try {
			ldapCtx.modifyAttributes(distinguishedName, updateMods);
		} catch (NamingException ne) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.updateFailed",distinguishedName,ne.getExplanation()); //$NON-NLS-1$
			throw new ConnectorException(msg);
		// don't remember why I added this generic catch of Exception,
		// but it does no harm...
		} catch (Exception e) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.updateFailedUnexpected",distinguishedName); //$NON-NLS-1$
			throw new ConnectorException(e, msg);
		}
	}

	// private method for extracting the distinguished name from
	// the criteria, which must include only an equals comparison
	// on the "DN" column ("WHERE DN='cn=John Doe,ou=people,dc=company,dc=com'") 
	// most of this code is to check the criteria to make sure it is in
	// this form and throw an appropriate exception if it is not
	// since there is no way to specify this granularity of criteria
	// right now in the connector capabilities
	private String getDNFromCriteria(ICriteria criteria)
			throws ConnectorException {
		if (criteria == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaEmptyError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		if (!(criteria instanceof ICompareCriteria)) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaNotSimpleError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		ICompareCriteria compareCriteria = (ICompareCriteria)criteria;	
		if (compareCriteria.getOperator() != Operator.EQ) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaNotEqualsError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		IExpression leftExpr = compareCriteria.getLeftExpression();
		if (!(leftExpr instanceof IElement)) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaLHSNotElementError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		// call utility method to get NameInSource/Name for element
		String nameLeftExpr = getNameFromElement((IElement)leftExpr);
		if (!(nameLeftExpr.toUpperCase().equals("DN"))) {   //$NON-NLS-1$
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaSrcColumnError",nameLeftExpr); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		IExpression rightExpr = compareCriteria.getRightExpression();
		if (!(rightExpr instanceof ILiteral)) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaRHSNotLiteralError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		Object valueRightExpr = ((ILiteral)rightExpr).getValue();
		if (!(valueRightExpr instanceof java.lang.String)) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaRHSNotStringError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		return (String)valueRightExpr;
	}

	// This is an exact copy of the method with the same name in
	// IQueryToLdapSearchParser - really should be in a utility class
	private String getNameFromElement(IElement e) throws ConnectorException {
		String ldapAttributeName = null;
		String elementNameDirect = e.getName();
		if (elementNameDirect == null) {
		} else {
		}
		MetadataObject mdObject = e.getMetadataObject();
		if (mdObject == null) {
			return "";  //$NON-NLS-1$
		}
		ldapAttributeName = mdObject.getNameInSource();
		if(ldapAttributeName == null || ldapAttributeName.equals("")) {	   //$NON-NLS-1$	
			ldapAttributeName = mdObject.getName();
			//	If name in source is not set, then fall back to the column name.
		}
		return ldapAttributeName;
	}


	// cancel here by closing the copy of the ldap context (if it was
	// initialized, which is only true if execute() was previously called)
	// calling close on already closed context is safe per
	// javax.naming.Context javadoc so we won't worry about this also
	// happening in our close method
	public void cancel() throws ConnectorException {
		close();
	}

	// close here by closing the copy of the ldap context (if it was
	// initialized, which is only true if execute() was previously called)
	// calling close on already closed context is safe per
	// javax.naming.Context javadoc so we won't worry about this also
	// happening in our close method
	public void close() throws ConnectorException {
		try {
			if(ldapCtx != null) {
				ldapCtx.close();
			}
		} catch (NamingException ne) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.closeContextError",ne.getExplanation()); //$NON-NLS-1$
			logger.logError(msg);
		}
	}

}
	



				
				
				
				
				
							
