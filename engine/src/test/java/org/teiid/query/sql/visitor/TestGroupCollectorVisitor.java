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

package org.teiid.query.sql.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;

import junit.framework.TestCase;


/**
 */
public class TestGroupCollectorVisitor extends TestCase {

	public TestGroupCollectorVisitor(String name) {
	    super(name);
	}	
	
	public GroupSymbol exampleGroupSymbol(int number) { 
		return new GroupSymbol("group." + number); //$NON-NLS-1$
	}
	
	public void helpTestGroups(LanguageObject obj, boolean removeDuplicates, Collection expectedGroups) {
		Collection actualGroups = GroupCollectorVisitor.getGroups(obj, removeDuplicates);
		assertEquals("Actual groups didn't meet expected groups: ", expectedGroups, actualGroups); //$NON-NLS-1$
	}
		
	public void testGroupSymbol() {
		GroupSymbol gs = exampleGroupSymbol(1);
		Set groups = new HashSet();
		groups.add(gs);
		helpTestGroups(gs, true, groups);
	}

	public void testUnaryFromClause() {
		GroupSymbol gs = exampleGroupSymbol(1);
		UnaryFromClause ufc = new UnaryFromClause(gs);
		Set groups = new HashSet();
		groups.add(gs);
		helpTestGroups(ufc, true, groups);
	}
	
	public void testJoinPredicate1() {
		GroupSymbol gs1 = exampleGroupSymbol(1);
		GroupSymbol gs2 = exampleGroupSymbol(2);
		JoinPredicate jp = new JoinPredicate(new UnaryFromClause(gs1), new UnaryFromClause(gs2), JoinType.JOIN_CROSS);
		
		Set groups = new HashSet();
		groups.add(gs1);
		groups.add(gs2);
		helpTestGroups(jp, true, groups);
	}

	public void testJoinPredicate2() {
		GroupSymbol gs1 = exampleGroupSymbol(1);
		GroupSymbol gs2 = exampleGroupSymbol(2);
		GroupSymbol gs3 = exampleGroupSymbol(3);
		JoinPredicate jp1 = new JoinPredicate(new UnaryFromClause(gs1), new UnaryFromClause(gs2), JoinType.JOIN_CROSS);
		JoinPredicate jp2 = new JoinPredicate(new UnaryFromClause(gs3), jp1, JoinType.JOIN_CROSS);
		
		Set groups = new HashSet();
		groups.add(gs1);
		groups.add(gs2);
		groups.add(gs3);
		helpTestGroups(jp2, true, groups);
	}
	
	public void testFrom1() {
		GroupSymbol gs1 = exampleGroupSymbol(1);
		GroupSymbol gs2 = exampleGroupSymbol(2);
		GroupSymbol gs3 = exampleGroupSymbol(3);
		From from = new From();
		from.addGroup(gs1);		    
		from.addGroup(gs2);		    
		from.addGroup(gs3);		    
		
		Set groups = new HashSet();
		groups.add(gs1);
		groups.add(gs2);
		groups.add(gs3);
		helpTestGroups(from, true, groups);
	}

	public void testFrom2() {
		GroupSymbol gs1 = exampleGroupSymbol(1);
		GroupSymbol gs2 = exampleGroupSymbol(2);
		GroupSymbol gs3 = exampleGroupSymbol(3);
		From from = new From();
		from.addGroup(gs1);		    
		from.addGroup(gs2);		    
		from.addGroup(gs3);		    
		
		List groups = new ArrayList();
		groups.add(gs1);
		groups.add(gs2);
		groups.add(gs3);
		helpTestGroups(from, false, groups);
	}

	public void testFrom3() {
		GroupSymbol gs1 = exampleGroupSymbol(1);
		GroupSymbol gs2 = exampleGroupSymbol(2);
		From from = new From();
		from.addGroup(gs1);		    
		from.addGroup(gs2);		    
		from.addGroup(gs2);		    
		
		Set groups = new HashSet();
		groups.add(gs1);
		groups.add(gs2);
		helpTestGroups(from, true, groups);
	}

	public void testFrom4() {
		GroupSymbol gs1 = exampleGroupSymbol(1);
		GroupSymbol gs2 = exampleGroupSymbol(2);
		From from = new From();
		from.addGroup(gs1);		    
		from.addGroup(gs2);		    
		from.addGroup(gs1);		    
		
		List groups = new ArrayList();
		groups.add(gs1);
		groups.add(gs2);
		groups.add(gs1);
		helpTestGroups(from, false, groups);
	}
	
	public void testInsert() {
		GroupSymbol gs1 = exampleGroupSymbol(1);
	 	Insert insert = new Insert();
	 	insert.setGroup(gs1);
	 	
	 	Set groups = new HashSet();
	 	groups.add(gs1);
	 	helpTestGroups(insert, true, groups);
	}

	public void testUpdate() {
		GroupSymbol gs1 = exampleGroupSymbol(1);
	 	Update update = new Update();
	 	update.setGroup(gs1);
	 	
	 	Set groups = new HashSet();
	 	groups.add(gs1);
	 	helpTestGroups(update, true, groups);
	}

	public void testDelete() {
		GroupSymbol gs1 = exampleGroupSymbol(1);
	 	Delete delete = new Delete();
	 	delete.setGroup(gs1);
	 	
	 	Set groups = new HashSet();
	 	groups.add(gs1);
	 	helpTestGroups(delete, true, groups);
	}
    
    public void testBatchedUpdateCommand() {
        GroupSymbol g1 = exampleGroupSymbol(1);
        GroupSymbol g2 = exampleGroupSymbol(2);
        GroupSymbol g3 = exampleGroupSymbol(3);
        Insert insert = new Insert();
        insert.setGroup(g1);
        Update update = new Update();
        update.setGroup(g2);
        Delete delete = new Delete();
        delete.setGroup(g3);
        
        List updates = new ArrayList(3);
        updates.add(insert);
        updates.add(update);
        updates.add(delete);
        
        Set groups = new HashSet();
        groups.add(g1);
        groups.add(g2);
        groups.add(g3);
        
        helpTestGroups(new BatchedUpdateCommand(updates), true, groups);
    }
}
