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

package com.metamatrix.query.validator;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.sql.lang.*;
import com.metamatrix.query.sql.symbol.*;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * <p> This visitor is used to validate updates through virtual groups. The command defining
 * the virtual group is always a <code>Query</code>. This object visits various parts of
 * this <code>Query</code> and verifies if the virtual group definition will allows it to be
 * updated.</p>
 */
public class UpdateValidationVisitor extends AbstractValidationVisitor {

	// metadata needed in validation process
	private QueryMetadataInterface metadata;

    // State during validation
    // collection of elementIDs defined on the SELECT clause
	private Collection elementsInSelect;

    /**
     * <p> This constructor initialises the visitor by setting the metadata
     * needed for validation.</p>
     * @param The metadata object needed for validation
     */
    public UpdateValidationVisitor(QueryMetadataInterface metadata) {
        super();
        this.metadata = metadata;
        this.elementsInSelect = new HashSet();
    }

    /**
     * This method get the metadata that this visitor uses.
     * @return The metadata object needed for validation
     */
    protected QueryMetadataInterface getMetadata() {
        return this.metadata;
    }

    // ############### Visitor methods for language objects ##################

    /**
     * <p> The command being visited should never be a <code>SetQuery</code> object, this method reports a
     * validation error if this mehod is visited.</p>
     * @param obj The <code>SetQuery</code> object to be visited for validation
     */
    public void visit(SetQuery obj) {
    	handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0001));
    }

    /**
     * <p> The command being visited should never be a <code>StoredProcedure</code> object, this method reports a
     * validation error if this mehod is visited.</p>
     * @param obj The <code>StoredProcedure</code> object to be visited for validation
     */
    public void visit(StoredProcedure obj) {
        handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0002));
    }

    /**
     * <p> The command being visited should never be a <code>Insert</code> object, this method reports a
     * validation error if this mehod is visited.</p>
     * @param obj The <code>Insert</code> object to be visited for validation
     */
    public void visit(Insert obj) {
        handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0003));
    }

    /**
     * <p> The command being visited should never be a <code>Update</code> object, this method reports a
     * validation error if this mehod is visited.</p>
     * @param obj The <code>Update</code> object to be visited for validation
     */
    public void visit(Update obj) {
        handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0004));
    }

    /**
     * <p> The command being visited should never be a <code>Delete</code> object, this method reports a
     * validation error if this mehod is visited.</p>
     * @param obj The <code>Delete</code> object to be visited for validation
     */
    public void visit(Delete obj) {
        handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0005));
    }

    /**
     * <p> This method visits the <code>Query</code> object and verifies that
     * it has only a Select and From clause.</p>
     * @param obj The <code>Query</code> object to be visited for validation
     */
    public void visit(Query obj) {
    	if((obj.getGroupBy() != null) || (obj.getHaving() != null)) {
    		handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0006));
    	}
    }

    /**
     * <p> This method visits the <code>Select</code> and verifies that the
     * expressions defined on it are all <code>ElementSymbol<code>s or aliased
     * <code>ElementSymbol<code>s</p>
     * @param obj The <code>Select</code> object to be visited for validation
     */
    public void visit(Select obj) {

    	Iterator elementIter = obj.getProjectedSymbols().iterator();

    	while(elementIter.hasNext()) {
            SingleElementSymbol symbol = (SingleElementSymbol) elementIter.next();
            if(symbol instanceof AliasSymbol) {
                symbol = ((AliasSymbol)symbol).getSymbol();
            }

            if(symbol instanceof AggregateSymbol) {
                handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0007, symbol));
            } else if(symbol instanceof ExpressionSymbol) {
                Expression expr = ((ExpressionSymbol)symbol).getExpression();
                if(expr == null || expr instanceof Function) {
                    handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0008, symbol));
                }
            }
    	}

        elementsInSelect = ElementCollectorVisitor.getElements(obj, false);
    }

    /**
     * <p> This method visits the <code>From</code> object to validate that
     * it has only one physical group in it. It gets the list of elements present
     * in the physical group but not specified in the Select clause and validates
     * these elements according the guidelines governing virtual group updates.</p>
     * @param obj The <code>From</code> object to be visited for validation
     */
    public void visit(From obj) {

    	Iterator groupIter = obj.getGroups().iterator();

    	GroupSymbol group = (GroupSymbol) groupIter.next();

    	if(groupIter.hasNext()) {
    		handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0009, obj.getGroups()));
    	} else {
			try {
				Object groupID = group.getMetadataID();
                if(groupID instanceof TempMetadataID) {
                    handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0002));
                } else {
    		    	Iterator elementsInGroupIter = getMetadata().getElementIDsInGroupID(groupID).iterator();
    				// walk through all the elements in the physical group
    		    	while(elementsInGroupIter.hasNext()) {
    		    		Object elementID = elementsInGroupIter.next();
                        ElementSymbol lookupSymbol = new ElementSymbol(getMetadata().getFullName(elementID));

    		    		// get the element that is not in the Select
    		    		if(!elementsInSelect.contains(lookupSymbol)) {
                            lookupSymbol.setMetadataID(elementID);

    			    		// validate the element is not required
                            validateElementNotRequired(lookupSymbol);
    			    	}
    			    }
                }
    		} catch(MetaMatrixException e) {
    			handleException(e);
    		}
    	}
    }

	/**
	 * <p> This method validates an elements present in the group specified in the
	 * FROM clause of the query but not specified in its SELECT clause, according to
	 * the rules governing virtul group updates.</p>
	 * @param element The <code>ElementSymbol</code> being validated
	 */
	private void validateElementNotRequired(ElementSymbol element) {

		try {
			// checking if the elements not specified in the query are required.
			if(getMetadata().elementSupports(element.getMetadataID(), SupportConstants.Element.NULL)) {
			    return;
			} else	if(getMetadata().elementSupports(element.getMetadataID(), SupportConstants.Element.DEFAULT_VALUE)) {
				return;
			} else if(getMetadata().elementSupports(element.getMetadataID(), SupportConstants.Element.AUTO_INCREMENT)) {
				return;
			}

			// this method should only be executed if the element is a required element
			// and none of cases above are true
		    handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0010, element));
		} catch(MetaMatrixException e) {
			handleException(e);
		}

	}
}
