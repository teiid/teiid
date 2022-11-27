/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.ldap;

import java.util.List;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.ldap.LdapContext;

import org.teiid.core.util.StringUtil;
import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;



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
public class LDAPUpdateExecution implements UpdateExecution {
    private LdapContext ldapConnection;
    private LdapContext ldapCtx;
    private Command command;

    public LDAPUpdateExecution(Command command, LdapContext ldapCtx) {
        this.ldapConnection = ldapCtx;
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
    public void execute() throws TranslatorException {
        // first make a copy of the initial LDAP context we got from
        // the connection.  The actual update-class operation will use
        // this copy.  This will enable the close and cancel methods
        // to stop any LDAP operations we are making by calling the
        // close() method of the copy context, without closing our
        // real connection to the LDAP server
        try {
            ldapCtx = (LdapContext)this.ldapConnection.lookup("");  //$NON-NLS-1$
        } catch (NamingException ne) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.createContextError",ne.getExplanation()); //$NON-NLS-1$
            throw new TranslatorException(ne, msg);
        }

        if (command instanceof Update) {
            executeUpdate();
        }
        else if (command instanceof Delete) {
            executeDelete();
        }
        else if (command instanceof Insert) {
            executeInsert();
        }
        else {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.incorrectCommandError"); //$NON-NLS-1$
            throw new TranslatorException(msg);
        }
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException,
            TranslatorException {
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
    // TODO - maybe automatically specify objectClass based off of
    // Name/NameInSource RESTRICT property settings, like with read support
    private void executeInsert()
            throws TranslatorException {

        List<ColumnReference> insertElementList = ((Insert)command).getColumns();
        List<Expression> insertValueList = ((ExpressionValueSource)((Insert)command).getValueSource()).getValues();
        // create a new attribute list with case ignored in attribute
        // names
        Attributes insertAttrs = new BasicAttributes(true);
        String distinguishedName = null;
        // The IInsert interface uses separate List objects for
        // the element names and values to be inserted, limiting
        // the potential for code reuse in reading them (since all
        // other interfaces use ICriteria-based mechanisms for such
        // input).
        for (int i=0; i < insertElementList.size(); i++) {
            ColumnReference insertElement = insertElementList.get(i);
            // call utility class to get NameInSource/Name of element
            String nameInsertElement = getNameFromElement(insertElement);
            // special handling for DN attribute - use it to set
            // distinguishedName value.
            Expression literal = insertValueList.get(i);
            if (nameInsertElement.toUpperCase().equals("DN")) {  //$NON-NLS-1$
                Object insertValue = ((Literal)literal).getValue();
                if (insertValue == null) {
                    final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.columnSourceNameDNNullError"); //$NON-NLS-1$
                    throw new TranslatorException(msg);
                }
                if (!(insertValue instanceof java.lang.String)) {
                    final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.columnSourceNameDNTypeError"); //$NON-NLS-1$
                    throw new TranslatorException(msg);
                }
                distinguishedName = (String)insertValue;
            }
            // for other attributes specified in the insert command,
            // create a new
            else {
                Attribute insertAttr = createBasicAttribute(nameInsertElement,
                        literal, insertElement.getMetadataObject());
                insertAttrs.put(insertAttr);
            }
        }
        // if the DN is not specified, we don't know enough to attempt
        // the LDAP add operation, so throw an exception
        if (distinguishedName == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.noInsertSourceNameDNError"); //$NON-NLS-1$
            throw new TranslatorException(msg);
        }
        // just try to create a new LDAP entry using the DN and
        // attributes specified in the INSERT operation.  If it isn't
        // legal, we'll get a NamingException back, whose explanation
        // we'll return in a ConnectorException
        try {
            ldapCtx.createSubcontext(distinguishedName, insertAttrs);
        } catch (NamingException ne) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.insertFailed",distinguishedName,ne.getExplanation()); //$NON-NLS-1$
            throw new TranslatorException(ne, msg);
        } catch (Exception e) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.insertFailedUnexpected",distinguishedName); //$NON-NLS-1$
            throw new TranslatorException(e, msg);
        }
    }

    static Attribute createBasicAttribute(String id,
            Expression expr, Column col) {
        Attribute attr = new BasicAttribute(id);
        if (expr instanceof org.teiid.language.Array) {
            List<Expression> exprs = ((org.teiid.language.Array)expr).getExpressions();
            for (Expression val : exprs) {
                Literal l = (Literal)val;
                if (l.getValue() != null) {
                    attr.add(IQueryToLdapSearchParser.getLiteralString(l));
                }
            }
        } else {
            Literal l = (Literal)expr;
            Object insertValue = null;
            if (l.getValue() != null) {
                if (LDAPQueryExecution.MULTIVALUED_CONCAT.equalsIgnoreCase(col.getDefaultValue())) {
                    List<String> vals = StringUtil.split(l.getValue().toString(), "?"); //$NON-NLS-1$
                    for (String val : vals) {
                        attr.add(val);
                    }
                    return attr;
                }
                insertValue = IQueryToLdapSearchParser.getLiteralString(l);
            }
            attr.add(insertValue);
        }
        return attr;
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
            throws TranslatorException {

        Condition criteria = ((Delete)command).getWhere();

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
            throw new TranslatorException(msg);
        // don't remember why I added this generic catch of Exception,
        // but it does no harm...
        } catch (Exception e) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.deleteFailedUnexpected",distinguishedName); //$NON-NLS-1$
            throw new TranslatorException(e, msg);
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

        List<SetClause> updateList = ((Update)command).getChanges();
        Condition criteria = ((Update)command).getWhere();

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
        // iterate through the supplied list of updates (each of
        // which is an ICompareCriteria with an IElement on the left
        // side and an IExpression on the right, per the Connector
        // API).
        for (int i=0; i < updateList.size(); i++) {
            SetClause setClause = updateList.get(i);
            // trust that connector API is right and left side
            // will always be an IElement
            ColumnReference leftElement = setClause.getSymbol();
            // call utility method to get NameInSource/Name for element
            String nameLeftElement = getNameFromElement(leftElement);
            // get right expression - if it is not a literal we
            // can't handle that so throw an exception
            Expression rightExpr = setClause.getValue();
            if (!(rightExpr instanceof Literal) && !(rightExpr instanceof org.teiid.language.Array)) {
                final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.valueNotLiteralError",nameLeftElement); //$NON-NLS-1$
                throw new TranslatorException(msg);
            }
            // add in the modification as a replacement - meaning
            // any existing value(s) for this attribute will
            // be replaced by the new value.  If the attribute
            // didn't exist, it will automatically be created
            // TODO - since null is a valid attribute
            // value, we don't do any special handling of it right
            // now.  But maybe null should mean to delete an
            // attribute?
            Attribute attribute = createBasicAttribute(nameLeftElement, rightExpr, leftElement.getMetadataObject());
            updateMods[i] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attribute);
        }
        // just try to update an LDAP entry using the DN and
        // attributes specified in the UPDATE operation.  If it isn't
        // legal, we'll get a NamingException back, whose explanation
        // we'll return in a ConnectorException
        try {
            ldapCtx.modifyAttributes(distinguishedName, updateMods);
        } catch (NamingException ne) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.updateFailed",distinguishedName,ne.getExplanation()); //$NON-NLS-1$
            throw new TranslatorException(ne, msg);
        // don't remember why I added this generic catch of Exception,
        // but it does no harm...
        } catch (Exception e) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.updateFailedUnexpected",distinguishedName); //$NON-NLS-1$
            throw new TranslatorException(e, msg);
        }
    }

    // private method for extracting the distinguished name from
    // the criteria, which must include only an equals comparison
    // on the "DN" column ("WHERE DN='cn=John Doe,ou=people,dc=company,dc=com'")
    // most of this code is to check the criteria to make sure it is in
    // this form and throw an appropriate exception if it is not
    // since there is no way to specify this granularity of criteria
    // right now in the connector capabilities
    private String getDNFromCriteria(Condition criteria)
            throws TranslatorException {
        if (criteria == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaEmptyError"); //$NON-NLS-1$
            throw new TranslatorException(msg);
        }
        if (!(criteria instanceof Comparison)) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaNotSimpleError"); //$NON-NLS-1$
            throw new TranslatorException(msg);
        }
        Comparison compareCriteria = (Comparison)criteria;
        if (compareCriteria.getOperator() != Operator.EQ) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaNotEqualsError"); //$NON-NLS-1$
            throw new TranslatorException(msg);
        }
        Expression leftExpr = compareCriteria.getLeftExpression();
        if (!(leftExpr instanceof ColumnReference)) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaLHSNotElementError"); //$NON-NLS-1$
            throw new TranslatorException(msg);
        }
        // call utility method to get NameInSource/Name for element
        String nameLeftExpr = getNameFromElement((ColumnReference)leftExpr);
        if (!(nameLeftExpr.toUpperCase().equals("DN"))) {   //$NON-NLS-1$
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaSrcColumnError",nameLeftExpr); //$NON-NLS-1$
            throw new TranslatorException(msg);
        }
        Expression rightExpr = compareCriteria.getRightExpression();
        if (!(rightExpr instanceof Literal)) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaRHSNotLiteralError"); //$NON-NLS-1$
            throw new TranslatorException(msg);
        }
        Object valueRightExpr = ((Literal)rightExpr).getValue();
        if (!(valueRightExpr instanceof java.lang.String)) {
            final String msg = LDAPPlugin.Util.getString("LDAPUpdateExecution.criteriaRHSNotStringError"); //$NON-NLS-1$
            throw new TranslatorException(msg);
        }
        return (String)valueRightExpr;
    }

    // This is an exact copy of the method with the same name in
    // IQueryToLdapSearchParser - really should be in a utility class
    private String getNameFromElement(ColumnReference e) {
        AbstractMetadataRecord mdObject = e.getMetadataObject();
        if (mdObject == null) {
            return "";  //$NON-NLS-1$
        }
        return mdObject.getSourceName();
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
        try {
            if(ldapCtx != null) {
                ldapCtx.close();
            }
        } catch (NamingException ne) {
            LogManager.logWarning(LogConstants.CTX_CONNECTOR, LDAPPlugin.Util.gs(LDAPPlugin.Event.TEIID12003, ne.getExplanation()));
        }
    }

}










