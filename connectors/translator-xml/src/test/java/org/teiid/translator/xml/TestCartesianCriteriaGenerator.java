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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.metadata.Column;


@SuppressWarnings("nls")
public class TestCartesianCriteriaGenerator {

	@Test
	public void testCartesian() throws Exception {
		String[] validValues =  {"ACE", "ADE", "BCE", "BDE", "ACF", "ADF", "BCF", "BDF"};
		
		List<CriteriaDesc> list = new ArrayList<CriteriaDesc>();
		
		Column c1 = new Column();
		c1.setName("c1");

		Column c2 = new Column();
		c2.setName("c2");
		
		Column c3 = new Column();
		c3.setName("c3");
		
		CriteriaDesc desc1 = new CriteriaDesc(c1, Arrays.asList(new String[] {"A", "B"}));
		CriteriaDesc desc2 = new CriteriaDesc(c2, Arrays.asList(new String[] {"C", "D"}));
		CriteriaDesc desc3 = new CriteriaDesc(c3, Arrays.asList(new String[] {"E", "F"}));
		list.add(desc1);
		list.add(desc2);
		list.add(desc3);
		
		List<List<CriteriaDesc>> catesianProduct = CartesienCriteriaGenerator.generateCartesianCriteria(list);
	
		assertEquals(8, catesianProduct.size());
		for (List<CriteriaDesc> cds:catesianProduct) {
			assertEquals(3, cds.size());
			
			String value = "";
			for (CriteriaDesc cd:cds) {
				List values = cd.getValues();
				for (Object v:values) {
					value = value+v.toString();
				}
			}
			assertTrue(Arrays.asList(validValues).contains(value));
		}
	}
		
	
	@Test
	public void testCartesianWithMultiValue() throws Exception {
		// the brackets represent the grouping, how they come as multi values.
		String[] validValues =  {"[A][C][EF]", "[A][D][EF]", "[B][C][EF]", "[B][D][EF]"};
		
		List<CriteriaDesc> list = new ArrayList<CriteriaDesc>();
		
		Column c1 = new Column();
		c1.setName("c1");

		Column c2 = new Column();
		c2.setName("c2");
		
		// make this column takes multi values
		Column c3 = new Column();
		c3.setName("c3");
		c3.setProperty(CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_COLUMN_PROPERTY_NAME, CriteriaDesc.PARM_HAS_MULTIPLE_VALUES_MULTI_ELEMENT_NAME);
		
		CriteriaDesc desc1 = new CriteriaDesc(c1, Arrays.asList(new String[] {"[A]", "[B]"}));
		CriteriaDesc desc2 = new CriteriaDesc(c2, Arrays.asList(new String[] {"[C]", "[D]"}));
		CriteriaDesc desc3 = new CriteriaDesc(c3, Arrays.asList(new String[] {"[E", "F]"})); 
		list.add(desc1);
		list.add(desc2);
		list.add(desc3);
		
		List<List<CriteriaDesc>> catesianProduct = CartesienCriteriaGenerator.generateCartesianCriteria(list);
	
		assertEquals(4, catesianProduct.size());
		for (List<CriteriaDesc> cds:catesianProduct) {
			assertEquals(3, cds.size());
			
			String value = "";
			for (CriteriaDesc cd:cds) {
				List values = cd.getValues();
				for (Object v:values) {
					value = value+v.toString();
				}
			}
			assertTrue(Arrays.asList(validValues).contains(value));
		}
	}	
	
}
