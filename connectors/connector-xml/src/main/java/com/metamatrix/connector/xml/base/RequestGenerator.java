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



package com.metamatrix.connector.xml.base;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RequestGenerator {

	// This method is misnamed. It generates cartesian products, not permutations.
	public static ArrayList getRequestPerms(List params)
	{
	    ArrayList soFar = new ArrayList();
	    
	    // Start off with a single "row" (with zero parameters)
		soFar.add(new CriteriaDesc[]{});
	    for (Iterator iter = params.iterator(); iter.hasNext(); ) {
	    	Object o = iter.next();
	        CriteriaDesc desc = (CriteriaDesc)o;
	        ArrayList nextGeneration = RequestGenerator.createCartesionProduct(soFar, desc);
	        soFar = nextGeneration;
	    }
	    
		return soFar;
	}

	// Create the cartesian product of a list of CriteriaDescs, and single CriteriaDesc
	// with (potentially) multiple values
	static ArrayList createCartesionProduct(List permsSoFar, CriteriaDesc desc)
	{
		ArrayList retval = new ArrayList();
	
		// Get the 'simple' cartesian product
		List rows = createCartesionProduct(permsSoFar, desc.getValues(), desc.isUnlimited());
		
		// Merge the existing list of CriteriaDescs with the new value turned into a CriteriaDesc)
		for (Iterator iter = rows.iterator(); iter.hasNext(); ) {
			Object oRow = iter.next();
			ArrayList row = (ArrayList)oRow;
			Object oOperand1 = row.get(0);
			CriteriaDesc[] previousCriteriaDescs = (CriteriaDesc[])oOperand1;
			
			CriteriaDesc[] newRow = new CriteriaDesc[previousCriteriaDescs.length + 1]; 
			System.arraycopy(previousCriteriaDescs, 0, newRow, 0, previousCriteriaDescs.length);
			CriteriaDesc singleValueCriteriaDesc = desc.cloneWithoutValues();
			for (int i=1; i < row.size(); i++ ){
	    		Object value = row.get(i);
	        	singleValueCriteriaDesc.setValue((i - 1), value);
			}
			newRow[newRow.length - 1] = singleValueCriteriaDesc;
			retval.add(newRow);
		}
	    return retval;
	}

	// Create the cartesian product of any two lists
	private static List createCartesionProduct(List operand1, List operand2, boolean multiElem)
	{
	    if (operand1.size() == 0) {
	    	operand1 = new ArrayList();
	    	operand1.add(null);
	    }
	
	    if (operand2.size() == 0) {
	    	operand2 = new ArrayList();
	    	operand2.add(null);
	    }
	
		
		ArrayList cartesianProduct = new ArrayList();
	    for (Iterator operand1iter = operand1.iterator(); operand1iter.hasNext(); ) {
	    	Object operand1item = operand1iter.next();
	
	    	if (! multiElem) {
	        	for (Iterator operand2iter = operand2.iterator(); operand2iter.hasNext(); ) {
		    		Object operand2item = operand2iter.next();
	
	     	    		ArrayList newRow = new ArrayList();
	                	newRow.add(operand1item);
	                    newRow.add(operand2item);
	                    cartesianProduct.add(newRow);            		
	            }
	        } else {
	        	ArrayList newRow = new ArrayList();
	        	newRow.add(operand1item);
	        	for (Iterator operand2iter = operand2.iterator(); operand2iter.hasNext(); ) {
		    		Object operand2item = operand2iter.next();
	                newRow.add(operand2item);
	        	}
	        	cartesianProduct.add(newRow);
	        }
	    }
	    return cartesianProduct;
	}

}
