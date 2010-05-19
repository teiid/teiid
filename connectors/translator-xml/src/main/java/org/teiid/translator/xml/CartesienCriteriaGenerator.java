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

/**
 * creates a Cartesian product of the Criteria descriptions based on their values.
 */
public class CartesienCriteriaGenerator {

	public static List<List<CriteriaDesc>> generateCartesianCriteria(List<CriteriaDesc> params) {
		List<List<CriteriaDesc>> cartesianProduct = new ArrayList<List<CriteriaDesc>>();

		for (CriteriaDesc desc : params) {
			createCartesionProduct(cartesianProduct, desc);
		}

		return cartesianProduct;
	}

	static void createCartesionProduct(List<List<CriteriaDesc>> cartesianProduct, CriteriaDesc desc) {
				
		List<CriteriaDesc> firstLevel = new ArrayList<CriteriaDesc>();
		if (desc.isUnlimited()) {
			firstLevel.add(desc);
		}
		else {
			for (Object value: desc.getValues()) {
				CriteriaDesc cd = desc.cloneWithoutValues();
				cd.setValue(0, value);
				firstLevel.add(cd);
			}
		}
		
		if (cartesianProduct.isEmpty()) {
			for (CriteriaDesc cd:firstLevel) {
				List<CriteriaDesc> secondLevel = new ArrayList<CriteriaDesc>();
				secondLevel.add(cd);
				cartesianProduct.add(secondLevel);
			}		
		}
		else {
			List<List<CriteriaDesc>> newcartesianProduct = new ArrayList<List<CriteriaDesc>>();
			
			for (CriteriaDesc cd:firstLevel) {
				for (List<CriteriaDesc> secondLevel:cartesianProduct) {
					
					List<CriteriaDesc> colnedSecondLevel = makeClone(secondLevel);
					colnedSecondLevel.add(cd);
					newcartesianProduct.add(colnedSecondLevel);
				}
			}
			cartesianProduct.clear();
			cartesianProduct.addAll(newcartesianProduct);
		}
	}

	private static List<CriteriaDesc> makeClone(List<CriteriaDesc> original){
		List<CriteriaDesc> clonedList = new ArrayList<CriteriaDesc>();
		
		for (CriteriaDesc cd:original) {
			CriteriaDesc clone = cd.cloneWithoutValues();
			int i = 0;
			for (Object value:cd.getValues()) {
				clone.setValue(i++, value);
			}
			clonedList.add(clone);
		}
		return clonedList;
	}
}
