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

package com.metamatrix.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.metamatrix.query.sql.lang.AbstractCompareCriteria;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.FromClause;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.JoinPredicate;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.proc.AssignmentStatement;
import com.metamatrix.query.sql.proc.Block;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.proc.CriteriaSelector;
import com.metamatrix.query.sql.proc.DeclareStatement;
import com.metamatrix.query.sql.proc.HasCriteria;
import com.metamatrix.query.sql.proc.IfStatement;
import com.metamatrix.query.sql.proc.Statement;
import com.metamatrix.query.sql.symbol.AllSymbol;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;

import junit.framework.TestCase;

public class TestOptionsAndHints extends TestCase {
    
    /** Select a From db.g1 MAKENOTDEP, db.g2 MAKENOTDEP WHERE a = b */
    public void testOptionMakeNotDepInline4(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("c", "db.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        
        CompareCriteria crit = new CompareCriteria(a, CompareCriteria.EQ, b);
        
        From from = new From();
        FromClause clause = new UnaryFromClause(g1);
        clause.setMakeNotDep(true);
        from.addClause(clause);
        FromClause clause1 = new UnaryFromClause(g2);
        clause1.setMakeNotDep(true);
        from.addClause(clause1);

        Select select = new Select();
        select.addSymbol(a);                

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        TestParser.helpTest("Select a From db.g1 MAKENOTDEP, db.g2 AS c MAKENOTDEP WHERE a = b",  //$NON-NLS-1$
                 "SELECT a FROM db.g1 MAKENOTDEP, db.g2 AS c MAKENOTDEP WHERE a = b",  //$NON-NLS-1$
                 query);
    }

    /** Select a From db.g1 JOIN db.g2 MAKEDEP ON a = b */
    public void testOptionMakeDepInline1(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        
        List crits = new ArrayList();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getRightClause().setMakeDep(true);
        From from = new From();
        from.addClause(jp);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From db.g1 JOIN db.g2 MAKEDEP ON a = b",  //$NON-NLS-1$
                 "SELECT a FROM db.g1 INNER JOIN db.g2 MAKEDEP ON a = b",  //$NON-NLS-1$
                 query);
    } 
    
    /** Select a From db.g1 MAKEDEP JOIN db.g2 ON a = b */
    public void testOptionMakeDepInline2(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        
        List crits = new ArrayList();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getLeftClause().setMakeDep(true);
        From from = new From();
        from.addClause(jp);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From db.g1 MAKEDEP JOIN db.g2 ON a = b",  //$NON-NLS-1$
                 "SELECT a FROM db.g1 MAKEDEP INNER JOIN db.g2 ON a = b",  //$NON-NLS-1$
                 query);
    }

    /** Select a From (db.g1 MAKEDEP JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 MAKEDEP ON a = c */
    public void testOptionMakeDepInline3(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        GroupSymbol g3 = new GroupSymbol("db.g3"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        ElementSymbol c = new ElementSymbol("c");  //$NON-NLS-1$
        
        List crits = new ArrayList();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getLeftClause().setMakeDep(true);
        List crits2 = new ArrayList();
        crits2.add(new CompareCriteria(a, CompareCriteria.EQ, c));
        JoinPredicate jp2 = new JoinPredicate(jp, new UnaryFromClause(g3), JoinType.JOIN_LEFT_OUTER, crits2);
        jp2.getRightClause().setMakeDep(true);
        From from = new From();
        from.addClause(jp2);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From (db.g1 MAKEDEP JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 MAKEDEP ON a = c",  //$NON-NLS-1$
                 "SELECT a FROM (db.g1 MAKEDEP INNER JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 MAKEDEP ON a = c",  //$NON-NLS-1$
                 query);
    }

    /** Select a From db.g1 MAKEDEP, db.g2 MAKEDEP WHERE a = b */
    public void testOptionMakeDepInline4(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("c", "db.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        
        CompareCriteria crit = new CompareCriteria(a, CompareCriteria.EQ, b);
        
        From from = new From();
        FromClause clause = new UnaryFromClause(g1);
        clause.setMakeDep(true);
        from.addClause(clause);
        FromClause clause1 = new UnaryFromClause(g2);
        clause1.setMakeDep(true);
        from.addClause(clause1);

        Select select = new Select();
        select.addSymbol(a);                

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(crit);
        TestParser.helpTest("Select a From db.g1 MAKEDEP, db.g2 AS c MAKEDEP WHERE a = b",  //$NON-NLS-1$
                 "SELECT a FROM db.g1 MAKEDEP, db.g2 AS c MAKEDEP WHERE a = b",  //$NON-NLS-1$
                 query);
    }

    public void testOptionMakedepBankOfAmerica() throws Exception {
        String sql = "SELECT A.alert_id " + //$NON-NLS-1$
            "FROM (FSK_ALERT AS A MAKEDEP INNER JOIN Core.FSC_PARTY_DIM AS C ON A.primary_entity_key = C.PARTY_KEY) " +//$NON-NLS-1$
            "LEFT OUTER JOIN FSK_SCENARIO AS S ON A.scenario_id = S.scenario_id " +//$NON-NLS-1$ 
            "OPTION PLANONLY DEBUG"; //$NON-NLS-1$
        Query command = (Query)new QueryParser().parseCommand(sql);
        JoinPredicate predicate = (JoinPredicate)command.getFrom().getClauses().get(0);
        assertTrue(((JoinPredicate)predicate.getLeftClause()).getLeftClause().isMakeDep());
    }
    
    /** Select a From db.g1 JOIN db.g2 MAKENOTDEP ON a = b */
    public void testOptionMakeNotDepInline1(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        
        List crits = new ArrayList();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getRightClause().setMakeNotDep(true);
        From from = new From();
        from.addClause(jp);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From db.g1 JOIN db.g2 MAKENOTDEP ON a = b",  //$NON-NLS-1$
                 "SELECT a FROM db.g1 INNER JOIN db.g2 MAKENOTDEP ON a = b",  //$NON-NLS-1$
                 query);
    } 
    
    /** Select a From db.g1 MAKENOTDEP JOIN db.g2 ON a = b */
    public void testOptionMakeNotDepInline2(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        
        List crits = new ArrayList();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getLeftClause().setMakeNotDep(true);
        From from = new From();
        from.addClause(jp);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From db.g1 MAKENOTDEP JOIN db.g2 ON a = b",  //$NON-NLS-1$
                 "SELECT a FROM db.g1 MAKENOTDEP INNER JOIN db.g2 ON a = b",  //$NON-NLS-1$
                 query);
    }

    /** Select a From (db.g1 MAKENOTDEP JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 MAKENOTDEP ON a = c */
    public void testOptionMakeNotDepInline3(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        GroupSymbol g3 = new GroupSymbol("db.g3"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        ElementSymbol c = new ElementSymbol("c");  //$NON-NLS-1$
        
        List crits = new ArrayList();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.getLeftClause().setMakeNotDep(true);
        List crits2 = new ArrayList();
        crits2.add(new CompareCriteria(a, CompareCriteria.EQ, c));
        JoinPredicate jp2 = new JoinPredicate(jp, new UnaryFromClause(g3), JoinType.JOIN_LEFT_OUTER, crits2);
        jp2.getRightClause().setMakeNotDep(true);
        From from = new From();
        from.addClause(jp2);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From (db.g1 MAKENOTDEP JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 MAKENOTDEP ON a = c",  //$NON-NLS-1$
                 "SELECT a FROM (db.g1 MAKENOTDEP INNER JOIN db.g2 ON a = b) LEFT OUTER JOIN db.g3 MAKENOTDEP ON a = c",  //$NON-NLS-1$
                 query);
    }

    public void testDepOptions2() {
        GroupSymbol a = new GroupSymbol("a"); //$NON-NLS-1$
        GroupSymbol b = new GroupSymbol("b"); //$NON-NLS-1$
        ElementSymbol x = new ElementSymbol("a.x", true); //$NON-NLS-1$
        ElementSymbol y = new ElementSymbol("b.y", true); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(x, CompareCriteria.EQ, new Function("function", new Expression[] {y})); //$NON-NLS-1$
        JoinPredicate predicate = new JoinPredicate(new UnaryFromClause(a), new UnaryFromClause(b), JoinType.JOIN_INNER, Arrays.asList(new Object[] {criteria}));
        From from = new From(Arrays.asList(new Object[] {predicate}));
        predicate.getLeftClause().setMakeNotDep(true);
        predicate.getRightClause().setMakeDep(true);
        Select select = new Select(Arrays.asList(new Object[] {x, y}));
        
        Query query = new Query(select, from, null, null, null, null, null);
        TestParser.helpTest("Select a.x, b.y From a MAKENOTDEP INNER JOIN b MAKEDEP ON a.x = function(b.y)",  //$NON-NLS-1$
                 "SELECT a.x, b.y FROM a MAKENOTDEP INNER JOIN b MAKEDEP ON a.x = function(b.y)",  //$NON-NLS-1$
                 query);
    }

    public void testOptionNoCache1(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.setNoCache(true);
        option.addNoCacheGroup("a.b.c"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option nocache a.b.c",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION NOCACHE a.b.c",  //$NON-NLS-1$
                 query);
    } 
    
    public void testOptionNoCache2(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.setNoCache(true);
        option.addNoCacheGroup("a.b.c"); //$NON-NLS-1$
        option.addNoCacheGroup("d.e.f"); //$NON-NLS-1$
        option.setShowPlan(true);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option nocache a.b.c, d.e.f showplan",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION SHOWPLAN NOCACHE a.b.c, d.e.f",  //$NON-NLS-1$
                 query);
    }   
    
//  related to defect 14423
    public void testOptionNoCache3(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.setNoCache(true); 

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option nocache",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION NOCACHE",  //$NON-NLS-1$
                 query);
    } 
    
    /** SELECT a from g OPTION xyx */
    public void testFailsIllegalOption(){
        TestParser.helpException("SELECT a from g OPTION xyx");         //$NON-NLS-1$
    }
    
    public void testInsertWithOption() {
        Insert insert = new Insert();
        insert.setGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        List vars = new ArrayList();
        vars.add(new ElementSymbol("a"));         //$NON-NLS-1$
        insert.setVariables(vars);
        List values = new ArrayList();
        values.add(new Reference(0));
        insert.setValues(values);
        Option option = new Option();
        option.setShowPlan(true);       
        insert.setOption(option);
        TestParser.helpTest("INSERT INTO m.g (a) VALUES (?) OPTION SHOWPLAN",  //$NON-NLS-1$
                 "INSERT INTO m.g (a) VALUES (?) OPTION SHOWPLAN",  //$NON-NLS-1$
                 insert);                     
    }
    
    public void testDeleteWithOption() {
        Delete delete = new Delete();
        delete.setGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        Option option = new Option();
        option.setShowPlan(true);       
        delete.setOption(option);
        TestParser.helpTest("DELETE FROM m.g OPTION SHOWPLAN",  //$NON-NLS-1$
                 "DELETE FROM m.g OPTION SHOWPLAN",  //$NON-NLS-1$
                 delete);                     
    }
    
    public void testUpdateWithOption() {
        Update update = new Update();     
        update.setGroup(new GroupSymbol("m.g")); //$NON-NLS-1$
        update.addChange(new ElementSymbol("a"), new Reference(0));
        Option option = new Option();
        option.setShowPlan(true);
        Criteria crit = new CompareCriteria(new ElementSymbol("b"), CompareCriteria.EQ, new Reference(1)); //$NON-NLS-1$
        update.setCriteria(crit);
        TestParser.helpTest("UPDATE m.g SET a = ? WHERE b = ? OPTION SHOWPLAN",  //$NON-NLS-1$
                 "UPDATE m.g SET a = ? WHERE b = ? OPTION SHOWPLAN",  //$NON-NLS-1$
                 update);                     
    }

    public void testOptionalFromClause1() {
        String sql = "SELECT * FROM /* optional */ t1, t2"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc); 
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT * FROM /* optional */ t1, t2", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause1_1() {
        String sql = "SELECT * FROM /* optional*/ t1, t2"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc); 
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT * FROM /* optional */ t1, t2", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause1_2() {
        String sql = "SELECT * FROM /*optional */ t1, t2"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc); 
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT * FROM /* optional */ t1, t2", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause1_3() {
        String sql = "SELECT * FROM /* optional  */ t1, t2"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc); 
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT * FROM /* optional */ t1, t2", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause1_4() {
        String sql = "SELECT * /* optional */ FROM /* OptiOnal  */ t1, t2"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc); 
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT * FROM /* optional */ t1, t2", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause1_5() {
        String sql = "SELECT * FROM /* OptiOnal  */ t1, t2"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc); 
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT * FROM /* optional */ t1, t2", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause2() {
        String sql = "SELECT * FROM t1, /* optional */ t2"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        ufc.setOptional(true);
        from.addClause(ufc); 
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT * FROM t1, /* optional */ t2", query);         //$NON-NLS-1$
    }

    public void testOptionalFromClause3() {
        String sql = "SELECT * FROM /* optional */ t1 AS a, t2"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("a", "t1")); //$NON-NLS-1$ //$NON-NLS-2$
        ufc.setOptional(true);
        from.addClause(ufc); 
        from.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT * FROM /* optional */ t1 AS a, t2", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause4() {
        String sql = "SELECT * FROM t1, /* optional */ t2 as a"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        from.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("a", "t2")); //$NON-NLS-1$ //$NON-NLS-2$
        ufc.setOptional(true);
        from.addClause(ufc); 
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT * FROM t1, /* optional */ t2 AS a", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause5() {
        String sql = "SELECT * FROM t1, /* optional */ (select * from t1, t2) as x"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        
        Query query2 = new Query();
        select = new Select();
        select.addSymbol(new AllSymbol());
        query2.setSelect(select);
        From from2 = new From();
        from2.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        from2.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query2.setFrom(from2);   
        
        SubqueryFromClause sfc = new SubqueryFromClause("x", query2);//$NON-NLS-1$
        sfc.setOptional(true);
        from.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        from.addClause(sfc);
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT * FROM t1, /* optional */ (SELECT * FROM t1, t2) AS x", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause6() {
        String sql = "SELECT * FROM t1 INNER JOIN /* optional */ (select a from t1, t2) AS x ON t1.a=x.a"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new AllSymbol());
        query.setSelect(select);
        From from = new From();
        
        Query query2 = new Query();
        select = new Select();
        select.addSymbol(new ElementSymbol("a"));//$NON-NLS-1$
        From from2 = new From();
        from2.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        from2.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query2.setSelect(select);
        query2.setFrom(from2);   
        
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$ 
        
        SubqueryFromClause sfc = new SubqueryFromClause("x", query2);//$NON-NLS-1$
        sfc.setOptional(true);
        
        List criteria = new ArrayList();
        criteria.add(new CompareCriteria(new ElementSymbol("t1.a"), AbstractCompareCriteria.EQ, new ElementSymbol("x.a")));//$NON-NLS-1$//$NON-NLS-2$
        JoinPredicate joinPredicate = new JoinPredicate(ufc, sfc, JoinType.JOIN_INNER, criteria);
        from.addClause(joinPredicate);
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT * FROM t1 INNER JOIN /* optional */ (SELECT a FROM t1, t2) AS x ON t1.a = x.a", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause7() {
        String sql = "SELECT b FROM t1, /* optional */ (t2 INNER JOIN t3 ON t2.a = t3.a)"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("b"));//$NON-NLS-1$
        query.setSelect(select);
        From from = new From(); 
        
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t2")); //$NON-NLS-1$ 
        
        UnaryFromClause ufc2 = new UnaryFromClause();
        ufc2.setGroup(new GroupSymbol("t3")); //$NON-NLS-1$
        
        List criteria = new ArrayList();
        criteria.add(new CompareCriteria(new ElementSymbol("t2.a"), AbstractCompareCriteria.EQ, new ElementSymbol("t3.a")));//$NON-NLS-1$//$NON-NLS-2$
        JoinPredicate joinPredicate = new JoinPredicate(ufc, ufc2, JoinType.JOIN_INNER, criteria);
        joinPredicate.setOptional(true);

        UnaryFromClause ufc3 = new UnaryFromClause();
        ufc3.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$ 
        from.addClause(ufc3);
        from.addClause(joinPredicate);
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT b FROM t1, /* optional */ (t2 INNER JOIN t3 ON t2.a = t3.a)", query);         //$NON-NLS-1$
    }

    public void testOptionalFromClause8() {
        String sql = "SELECT b FROM t1, /* optional */ (/* optional */ (SELECT * FROM t1, t2) AS x INNER JOIN t3 ON x.a = t3.a)"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("b"));//$NON-NLS-1$
        query.setSelect(select);
        From from = new From(); 
        
        
        Query query2 = new Query();
        select = new Select();
        select.addSymbol(new AllSymbol());
        From from2 = new From();
        from2.addGroup(new GroupSymbol("t1")); //$NON-NLS-1$
        from2.addGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        query2.setSelect(select);
        query2.setFrom(from2);
        SubqueryFromClause sfc = new SubqueryFromClause("x", query2);//$NON-NLS-1$
        sfc.setOptional(true);
        
        UnaryFromClause ufc2 = new UnaryFromClause();
        ufc2.setGroup(new GroupSymbol("t3")); //$NON-NLS-1$
        
        List criteria = new ArrayList();
        criteria.add(new CompareCriteria(new ElementSymbol("x.a"), AbstractCompareCriteria.EQ, new ElementSymbol("t3.a")));//$NON-NLS-1$//$NON-NLS-2$
        JoinPredicate joinPredicate = new JoinPredicate(sfc, ufc2, JoinType.JOIN_INNER, criteria);
        joinPredicate.setOptional(true);

        UnaryFromClause ufc3 = new UnaryFromClause();
        ufc3.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$ 
        from.addClause(ufc3);
        from.addClause(joinPredicate);
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT b FROM t1, /* optional */ (/* optional */ (SELECT * FROM t1, t2) AS x INNER JOIN t3 ON x.a = t3.a)", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause9() {
        String sql = "SELECT b FROM (t1 LEFT OUTER JOIN /* optional */t2 on t1.a = t2.a) LEFT OUTER JOIN /* optional */t3 on t1.a = t3.a"; //$NON-NLS-1$
        
        Query query = new Query();
        Select select = new Select();
        select.addSymbol(new ElementSymbol("b"));//$NON-NLS-1$
        query.setSelect(select);
        From from = new From(); 
        
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("t1")); //$NON-NLS-1$ 
        
        UnaryFromClause ufc2 = new UnaryFromClause();
        ufc2.setGroup(new GroupSymbol("t2")); //$NON-NLS-1$
        ufc2.setOptional(true);
        
        List criteria = new ArrayList();
        criteria.add(new CompareCriteria(new ElementSymbol("t1.a"), AbstractCompareCriteria.EQ, new ElementSymbol("t2.a")));//$NON-NLS-1$//$NON-NLS-2$
        JoinPredicate joinPredicate = new JoinPredicate(ufc, ufc2, JoinType.JOIN_LEFT_OUTER, criteria);

        UnaryFromClause ufc3 = new UnaryFromClause();
        ufc3.setGroup(new GroupSymbol("t3")); //$NON-NLS-1$
        ufc3.setOptional(true);

        criteria = new ArrayList();
        criteria.add(new CompareCriteria(new ElementSymbol("t1.a"), AbstractCompareCriteria.EQ, new ElementSymbol("t3.a")));//$NON-NLS-1$//$NON-NLS-2$
        JoinPredicate joinPredicate2 = new JoinPredicate(joinPredicate, ufc3, JoinType.JOIN_LEFT_OUTER, criteria);

        from.addClause(joinPredicate2);
        query.setFrom(from);           

        TestParser.helpTest(sql, "SELECT b FROM (t1 LEFT OUTER JOIN /* optional */ t2 ON t1.a = t2.a) LEFT OUTER JOIN /* optional */ t3 ON t1.a = t3.a", query);         //$NON-NLS-1$
    }
    
    public void testOptionalFromClause10(){
        //declare var1
        ElementSymbol var1 = new ElementSymbol("var1"); //$NON-NLS-1$
        String shortType = new String("short"); //$NON-NLS-1$
        Statement declStmt = new DeclareStatement(var1, shortType);
        
        //ifblock
        List symbols = new ArrayList();
        symbols.add(new ElementSymbol("a1"));  //$NON-NLS-1$
        Select select = new Select(symbols);       
        
        From from = new From();
        from.addGroup(new GroupSymbol("g")); //$NON-NLS-1$
        
        Criteria criteria = new CompareCriteria(new ElementSymbol("a2"), CompareCriteria.EQ,  //$NON-NLS-1$
            new Constant(new Integer(5)));
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setCriteria(criteria);
        
        Command queryCmd = query;
        AssignmentStatement queryStmt = new AssignmentStatement(var1, queryCmd);
              
        Block ifBlock = new Block();      
        ifBlock.addStatement(queryStmt);
        
        //else block 
        ElementSymbol var2 = new ElementSymbol("var2"); //$NON-NLS-1$
        Statement elseDeclStmt = new DeclareStatement(var2, shortType);     
        
        List elseSymbols = new ArrayList();
        elseSymbols.add(new ElementSymbol("b1"));  //$NON-NLS-1$
        Select elseSelect = new Select(elseSymbols); 
    
        Query elseQuery = new Query();
        elseQuery.setSelect(elseSelect);
        From elseFrom = (From)from.clone();
        UnaryFromClause ufc = new UnaryFromClause();
        ufc.setGroup(new GroupSymbol("h")); //$NON-NLS-1$ 
        ufc.setOptional(true);
        elseFrom.addClause(ufc);
        elseQuery.setFrom(elseFrom);
        elseQuery.setCriteria(criteria);
        
        Command elseQueryCmd = elseQuery;
        AssignmentStatement elseQueryStmt = new AssignmentStatement(var2, elseQueryCmd);
        
        Block elseBlock = new Block();
        List elseStmts = new ArrayList();
        elseStmts.add(elseDeclStmt);
        elseStmts.add(elseQueryStmt);
      
        elseBlock.setStatements(elseStmts);
   
        //has criteria
        ElementSymbol a = new ElementSymbol("a"); //$NON-NLS-1$
        List elements = new ArrayList();
        elements.add(a);
        
        CriteriaSelector critSelector = new CriteriaSelector();
        critSelector.setSelectorType(CriteriaSelector.IN);
        critSelector.setElements(elements);
        
        HasCriteria hasSelector = new HasCriteria();
        hasSelector.setSelector(critSelector);
        
        IfStatement stmt = new IfStatement(hasSelector, ifBlock, elseBlock);
        
        Block block = new Block();        
        block.addStatement(declStmt);
        block.addStatement(stmt);
                
        CreateUpdateProcedureCommand cmd = new CreateUpdateProcedureCommand();
        cmd.setBlock(block);
       
        TestParser.helpTest("CREATE PROCEDURE BEGIN DECLARE short var1;"+ //$NON-NLS-1$
           " IF(HAS IN CRITERIA ON (a)) BEGIN var1 = SELECT a1 FROM g WHERE a2 = 5; END"+ //$NON-NLS-1$
           " ELSE BEGIN DECLARE short var2; var2 = SELECT b1 FROM g, /* optional */ h WHERE a2 = 5; END" + //$NON-NLS-1$
           " END", "CREATE PROCEDURE"+"\n"+"BEGIN"+"\n"+"DECLARE short var1;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
           "IF(HAS IN CRITERIA ON (a))"+"\n"+"BEGIN"+"\n"+ "var1 = SELECT a1 FROM g WHERE a2 = 5;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
           "END"+"\n"+"ELSE"+"\n"+"BEGIN"+"\n"+"DECLARE short var2;"+"\n"+ //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
           "var2 = SELECT b1 FROM g, /* optional */ h WHERE a2 = 5;"+"\n"+"END"+"\n"+"END", cmd);                      //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }

    public void testStoredQueryWithOption(){
        StoredProcedure storedQuery = new StoredProcedure();
        storedQuery.setProcedureName("proc1"); //$NON-NLS-1$
        Option option = new Option();
        option.setDebug(true);
        storedQuery.setOption(option);
        TestParser.helpTest("exec proc1() option debug", "EXEC proc1() OPTION DEBUG", storedQuery); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** Select a From db.g Option SHOWPLAN */
    public void testOptionShowPlan(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option showplan = new Option();
        showplan.setShowPlan(true);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(showplan);
        TestParser.helpTest("Select a From db.g Option SHOWPLAN",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION SHOWPLAN",  //$NON-NLS-1$
                 query);
    }   

    /** Select a From db.g Option PLANONLY */
    public void testOptionPlanOnly(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option showplan = new Option();
        showplan.setPlanOnly(true);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(showplan);
        TestParser.helpTest("Select a From db.g Option planOnly",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION PLANONLY",  //$NON-NLS-1$
                 query);
    }   

    /** Select a From db.g Option makedep a.b.c */
    public void testOptionMakeDependent1(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addDependentGroup("a.b.c"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makedep a.b.c",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION MAKEDEP a.b.c",  //$NON-NLS-1$
                 query);
    }   

    /** Select a From db.g Option makedep a.b.c, d.e.f showplan */
    public void testOptionMakeDependent2(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addDependentGroup("a.b.c"); //$NON-NLS-1$
        option.addDependentGroup("d.e.f"); //$NON-NLS-1$
        option.setShowPlan(true);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makedep a.b.c, d.e.f showplan",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION SHOWPLAN MAKEDEP a.b.c, d.e.f",  //$NON-NLS-1$
                 query);
    }   

    /** Select a From db.g Option makedep a.b.c, d.e.f, x.y.z */
    public void testOptionMakeDependent3(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addDependentGroup("a.b.c"); //$NON-NLS-1$
        option.addDependentGroup("d.e.f"); //$NON-NLS-1$
        option.addDependentGroup("x.y.z"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makedep a.b.c, d.e.f, x.y.z",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION MAKEDEP a.b.c, d.e.f, x.y.z",  //$NON-NLS-1$
                 query);
    }   

    /** Select a From db.g Option makenotdep a.b.c */
    public void testOptionMakeNotDependent1(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addNotDependentGroup("a.b.c"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makenotdep a.b.c",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION MAKENOTDEP a.b.c",  //$NON-NLS-1$
                 query);
    }   

    /** Select a From db.g Option makenotdep a.b.c, d.e.f showplan */
    public void testOptionMakeNotDependent2(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addNotDependentGroup("a.b.c"); //$NON-NLS-1$
        option.addNotDependentGroup("d.e.f"); //$NON-NLS-1$
        option.setShowPlan(true);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makeNOTdep a.b.c, d.e.f showplan",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION SHOWPLAN MAKENOTDEP a.b.c, d.e.f",  //$NON-NLS-1$
                 query);
    }   

    /** Select a From db.g Option makenotdep a.b.c, d.e.f, x.y.z */
    public void testOptionMakeNotDependent3(){
        GroupSymbol g = new GroupSymbol("db.g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        Select select = new Select();
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        select.addSymbol(a);

        Option option = new Option();
        option.addNotDependentGroup("a.b.c"); //$NON-NLS-1$
        option.addNotDependentGroup("d.e.f"); //$NON-NLS-1$
        option.addNotDependentGroup("x.y.z"); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setOption(option);
        TestParser.helpTest("Select a From db.g Option makenotdep a.b.c, d.e.f, x.y.z",  //$NON-NLS-1$
                 "SELECT a FROM db.g OPTION MAKENOTDEP a.b.c, d.e.f, x.y.z",  //$NON-NLS-1$
                 query);
    }
    
    public void testDepOptions1() {
        GroupSymbol a = new GroupSymbol("a"); //$NON-NLS-1$
        GroupSymbol b = new GroupSymbol("b"); //$NON-NLS-1$
        ElementSymbol x = new ElementSymbol("a.x", true); //$NON-NLS-1$
        ElementSymbol y = new ElementSymbol("b.y", true); //$NON-NLS-1$
        
        From from = new From(Arrays.asList(new Object[] {new UnaryFromClause(a), new UnaryFromClause(b)}));
        
        Option option = new Option();
        option.addDependentGroup("a"); //$NON-NLS-1$
        option.addNotDependentGroup("b"); //$NON-NLS-1$
        
        Select select = new Select(Arrays.asList(new Object[] {x, y}));
        
        Criteria criteria = new CompareCriteria(x, CompareCriteria.EQ, y);
        Query query = new Query(select, from, criteria, null, null, null, option);
        TestParser.helpTest("Select a.x, b.y From a, b WHERE a.x = b.y option makedep a makenotdep b",  //$NON-NLS-1$
                 "SELECT a.x, b.y FROM a, b WHERE a.x = b.y OPTION MAKEDEP a MAKENOTDEP b",  //$NON-NLS-1$
                 query);
    }
    
    public void testOptionMakeDepInline5(){
        GroupSymbol g1 = new GroupSymbol("db.g1"); //$NON-NLS-1$
        GroupSymbol g2 = new GroupSymbol("db.g2"); //$NON-NLS-1$
        GroupSymbol g3 = new GroupSymbol("db.g3"); //$NON-NLS-1$
        ElementSymbol a = new ElementSymbol("a");  //$NON-NLS-1$
        ElementSymbol b = new ElementSymbol("b");  //$NON-NLS-1$
        ElementSymbol c = new ElementSymbol("c");  //$NON-NLS-1$
        
        List crits = new ArrayList();
        crits.add(new CompareCriteria(a, CompareCriteria.EQ, b));
        JoinPredicate jp = new JoinPredicate(new UnaryFromClause(g1), new UnaryFromClause(g2), JoinType.JOIN_INNER, crits);
        jp.setMakeDep(true);
        List crits2 = new ArrayList();
        crits2.add(new CompareCriteria(a, CompareCriteria.EQ, c));
        JoinPredicate jp2 = new JoinPredicate(jp, new UnaryFromClause(g3), JoinType.JOIN_LEFT_OUTER, crits2);
        From from = new From();
        from.addClause(jp2);

        Select select = new Select();
        select.addSymbol(a);

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        TestParser.helpTest("Select a From (db.g1 JOIN db.g2 ON a = b) makedep LEFT OUTER JOIN db.g3 ON a = c",  //$NON-NLS-1$
                 "SELECT a FROM (db.g1 INNER JOIN db.g2 ON a = b) MAKEDEP LEFT OUTER JOIN db.g3 ON a = c",  //$NON-NLS-1$
                 query);
        
        //ensure that the new string form is parsable
        TestParser.helpTest(query.toString(), query.toString(), query);
    }
    
}
