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
package org.teiid.translator.jpa;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Select;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestJSelectJPQLVisitor {
    private JPA2ExecutionFactory jpaTranslator;
    private TranslationUtility utility;

    @Before
    public void setUp() throws Exception {
    	jpaTranslator = new JPA2ExecutionFactory();
    	jpaTranslator.start();
    	
    	TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("sakila.ddl")), "sakila", "sakila");
    	utility = new TranslationUtility(metadata);
    	
    }
    
    private void helpExecute(String query, String expected) throws Exception {
    	Select cmd = (Select)this.utility.parseCommand(query);
    	String jpaCommand = JPQLSelectVisitor.getJPQLString(cmd, jpaTranslator, utility.createRuntimeMetadata());
    	assertEquals(expected, jpaCommand);
    }
    
    @Test
    public void testProjectionBasedJoin() throws Exception {
    	helpExecute("select * from customer as c", "SELECT c.customer_id, J_0.store_id, c.first_name, c.last_name, c.email, J_1.address_id, c.active, c.create_date, c.last_update FROM customer AS c INNER JOIN c.store AS J_0 INNER JOIN c.address AS J_1");
    }
    
    @Test
    public void testExplicitJoinJoin() throws Exception {
    	helpExecute("select c.first_name, c.last_name, a.address_id FROM customer c join address a on c.address_id=a.address_id", 
    			"SELECT c.first_name, c.last_name, a.address_id FROM customer AS c INNER JOIN c.address AS a");
    }
    
    @Test
    public void testSimpleSelect() throws Exception {
    	helpExecute("select c.first_name, c.last_name FROM customer c order by last_name", 
    			"SELECT c.first_name, c.last_name FROM customer AS c ORDER BY c.last_name");
    }
    
    @Test
    public void testFunctionsSelect() throws Exception {
    	helpExecute("select concat(lcase(first_name), last_Name) from customer as c", 
    			"SELECT concat(lower(c.first_name), c.last_name) FROM customer AS c");
    }
    
    @Test
    public void testRightJoinRewriteSelect() throws Exception {
    	helpExecute("select c.first_name, c.last_name, c.address_id, a.phone from customer c right join address a on c.address_Id=a.address_Id", 
    			"SELECT c.first_name, c.last_name, a.address_id, a.phone FROM customer AS c RIGHT OUTER JOIN c.address AS a");
    }
    
    @Test
    public void testRightandinnerJoinRewriteSelect() throws Exception {
    	helpExecute("select c.first_name, c.last_name, a.address_id, a.phone, ci.city from customer c join address a on c.address_Id=a.address_Id right join city ci on ci.city_id = a.city_id where c.last_Name='MYERS' OR c.last_Name='TALBERT' order by c.first_Name", 
    			"SELECT c.first_name, c.last_name, a.address_id, a.phone, ci.city FROM customer AS c INNER JOIN c.address AS a RIGHT OUTER JOIN a.city AS ci WHERE c.last_name IN ('TALBERT', 'MYERS') ORDER BY c.first_name");
    }
    
	// needs one with composite PK
}
