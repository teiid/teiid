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

package com.metamatrix.query.sql.util;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.sql.ProcedureReservedWords;
import com.metamatrix.query.sql.lang.*;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.proc.*;
import com.metamatrix.query.sql.symbol.*;
import com.metamatrix.query.util.ErrorMessageKeys;
import com.metamatrix.query.validator.UpdateValidationVisitor;
import com.metamatrix.query.validator.ValidatorReport;

/**
 * Use existing query transformation to create a simple insert/update/delete procedure.
 * Only works for the virtual group that maps to single physical group.
 * No expression is allowed in SELECT statement.
 * All required elements must be specified.
 */
public class UpdateProcedureGenerator {

	// constant for an Insert procedure determining the procedure type
	public static final int INSERT_PROCEDURE = 1;

	// constant for an Update procedure determining the procedure type
	public static final int UPDATE_PROCEDURE = 2;

	// constant for an Delete procedure determining the procedure type
	public static final int DELETE_PROCEDURE = 3;

	/**
	 * Create CreateUpdateProcedureCommand for the specific virtual group.
     * @return Generated procedure or null if no procedure could be generated
	 */
	public static CreateUpdateProcedureCommand createProcedure (
			int procedureType, String virtualGroup, Command queryTransformation, QueryMetadataInterface metadata)
			throws MetaMatrixComponentException, QueryMetadataException {

        // validate that a procedure can be generated
        UpdateValidationVisitor updateVisitor = new UpdateValidationVisitor(metadata);
        PreOrderNavigator.doVisit(queryTransformation, updateVisitor);
        ValidatorReport report = updateVisitor.getReport();
        if(report.hasItems()) {
            return null;
        }

		//the command should be a query
		Query query = (Query)queryTransformation;
		//get a list of symbols in select statement
		List selectSymbols = query.getProjectedSymbols();
        
        if(query.getFrom() == null) {
            return null;
        }

		GroupSymbol pGroup = (GroupSymbol)query.getFrom().getGroups().iterator().next();
        String pGroupName = pGroup.getName();
        if(pGroup.getDefinition() != null) {
            pGroupName = pGroup.getDefinition();
        }

        //get a list of the elements in the virtual group
        List elementsInVG = metadata.getElementIDsInGroupID(metadata.getGroupID(virtualGroup));

        // Create symbol for the ROWS_UPDATED special variable
        ElementSymbol rowsUpdated = new ElementSymbol(ProcedureReservedWords.ROWS_UPDATED);

		CreateUpdateProcedureCommand cupc = null;
		switch(procedureType){
			case INSERT_PROCEDURE:
            {
				List variables = new ArrayList();
				List values = new ArrayList();
				mapElements(selectSymbols, elementsInVG, pGroupName, metadata, variables, values);
				Insert insert = new Insert();
				insert.setGroup(new GroupSymbol(pGroupName));
				insert.setVariables(variables);
				insert.setValues(values);
                AssignmentStatement assignStmt = new AssignmentStatement(rowsUpdated, insert);
				Block b = new Block();
    			b.addStatement(assignStmt);
    			cupc = new CreateUpdateProcedureCommand(b);
				break;
            }
			case UPDATE_PROCEDURE:
            {
                List variables = new ArrayList();
                List values = new ArrayList();
				mapElements(selectSymbols, elementsInVG, pGroupName, metadata, variables, values);
				Update update = new Update();
				update.setGroup(new GroupSymbol(pGroupName));
				for(int i = 0; i < variables.size(); i++){
					ElementSymbol variable = (ElementSymbol)variables.get(i);
					Expression value = (Expression)values.get(i);
					update.addChange(variable, value);
				}
				update.setCriteria(new TranslateCriteria(new CriteriaSelector()));
                AssignmentStatement assignStmt = new AssignmentStatement(rowsUpdated, update);
				Block b = new Block();
    			b.addStatement(assignStmt);
    			cupc = new CreateUpdateProcedureCommand(b);
				break;
            }
			case DELETE_PROCEDURE:
            {
				Delete delete = new Delete();
				delete.setGroup(new GroupSymbol(pGroupName));
				delete.setCriteria(new TranslateCriteria(new CriteriaSelector()));
                AssignmentStatement assignStmt = new AssignmentStatement(rowsUpdated, delete);
				Block b = new Block();
    			b.addStatement(assignStmt);
    			cupc = new CreateUpdateProcedureCommand(b);
				break;
            }
			default:
				//should not come here
                break;
		}

		return cupc;
	}

    /**
     * Virtual elements and projected symbols should match up 1-to-1.  Short names
     * of both are the same.  We want to build a mapping where the variables are
     * the underlying physical element and the value is the INPUT variable for the
     * virtual element.
     *
     * @param physicalElements Projected symbols from transformation query
     * @param metadata Metadata access
     * @param variables Collect each variable (physical element being updated)
     * @param values Collect each value (INPUT value for respective virtual element)
     */
    private static void mapElements(List physicalElements, List virtualElements, String physicalGroup, QueryMetadataInterface metadata, List variables, List values)
            throws MetaMatrixComponentException, QueryMetadataException{

        if(physicalElements.size()!= virtualElements.size()) {
            throw new QueryMetadataException(ErrorMessageKeys.SQL_0018, QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0018));
        }

        //match the physical group elements to the virtual group elements
        for(int i=0; i<physicalElements.size(); i++) {
            // Strip alias if necessary to get physical element
            SingleElementSymbol pSymbol = (SingleElementSymbol)physicalElements.get(i);
            if(pSymbol instanceof AliasSymbol) {
                pSymbol = ((AliasSymbol) pSymbol).getSymbol();
            }

            if(pSymbol instanceof ElementSymbol) {
                final Object mid = ((ElementSymbol)pSymbol).getMetadataID();
                final boolean supportsUpdate = metadata.elementSupports(mid, SupportConstants.Element.UPDATE);
                //Only include elements that are updateable.
                if(supportsUpdate) {
                    // Create properly named physical element
                    String properName = metadata.getFullElementName(physicalGroup, pSymbol.getShortName());
                    variables.add(new ElementSymbol(properName));
    
                    // Construct properly named INPUT variable based on short name of virtual element
                    String virtualElementName = metadata.getFullName(virtualElements.get(i));
                    String virtualElementShortName = metadata.getShortElementName(virtualElementName);
                    ElementSymbol inputElement = new ElementSymbol(ProcedureReservedWords.INPUT + "." + virtualElementShortName); //$NON-NLS-1$
                    values.add(inputElement);
                }
            }
        }
    }

}
