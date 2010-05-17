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



package org.teiid.translator.xml;

import java.util.ArrayList;
import java.util.List;

public class RequestGenerator {

	// This method is misnamed. It generates cartesian products, not permutations.
	public static List<CriteriaDesc[]> getRequests(List<CriteriaDesc> params)
	{
	    List<CriteriaDesc[]> soFar = new ArrayList<CriteriaDesc[]>();
	    
	    // Start off with a single "row" (with zero parameters)
		soFar.add(new CriteriaDesc[]{});
	    for (CriteriaDesc desc: params){
	    	soFar = RequestGenerator.createCartesionProduct(soFar, desc);
	    }
	    
		return soFar;
	}

	// Create the cartesian product of a list of CriteriaDescs, and single CriteriaDesc
	// with (potentially) multiple values
	static List<CriteriaDesc[]> createCartesionProduct(List<CriteriaDesc[]> permsSoFar, CriteriaDesc desc)
	{
		List<CriteriaDesc[]> retval = new ArrayList<CriteriaDesc[]>();
	
		// Get the 'simple' cartesian product
		List<List> rows = createCartesionProduct(permsSoFar, desc.getValues(), desc.isUnlimited());
		
		// Merge the existing list of CriteriaDescs with the new value turned into a CriteriaDesc)
		for (List row : rows) {
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
	private static List<List> createCartesionProduct(List<CriteriaDesc[]> operand1, List operand2, boolean multiElem)
	{
	    if (operand1.size() == 0) {
	    	operand1 = new ArrayList<CriteriaDesc[]>();
	    	operand1.add(null);
	    }
	
	    if (operand2.size() == 0) {
	    	operand2 = new ArrayList();
	    	operand2.add(null);
	    }
	
		
	    List<List> cartesianProduct = new ArrayList<List>();
	    for (CriteriaDesc[] operand1item : operand1) {
	    	List newRow = new ArrayList();
	    	if (! multiElem) {
	        	for (Object operand2item : operand2 ) {
		    		newRow.add(operand1item);
	                newRow.add(operand2item);
	                cartesianProduct.add(newRow);            		
	            }
	        } else {
	        	newRow.add(operand1item);
	        	for (Object operand2item : operand2 ) {
		    		newRow.add(operand2item);
	        	}
	        	cartesianProduct.add(newRow);
	        }
	    }
	    return cartesianProduct;
	}

}
