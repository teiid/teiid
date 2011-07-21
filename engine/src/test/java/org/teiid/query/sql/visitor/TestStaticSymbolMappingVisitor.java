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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.BetweenCriteria;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.proc.CriteriaSelector;
import org.teiid.query.sql.proc.TranslateCriteria;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.Symbol;



public class TestStaticSymbolMappingVisitor extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestStaticSymbolMappingVisitor(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################	
		
	private Map getSymbolMap() {
		HashMap map = new HashMap();
		map.put(exampleElement(true, 0), exampleElement(false, 0));
		map.put(exampleElement(true, 1), exampleElement(false, 1));
		map.put(exampleElement(true, 2), exampleElement(false, 2));
		map.put(exampleGroup(true, 0), exampleGroup(false, 0));
		map.put(exampleGroup(true, 1), exampleGroup(false, 1));
		
		return map;    
	}		

	private ElementSymbol exampleElement(boolean old, int i) {
	    ElementSymbol element = null;
	    if(old) { 
			element = new ElementSymbol("OLDE" + i); //$NON-NLS-1$
	    } else {
			element = new ElementSymbol("NEWE" + i); //$NON-NLS-1$
	    }
	    return element;
	}

	private GroupSymbol exampleGroup(boolean old, int i) {
	    if(old) { 
			return new GroupSymbol("OLDG" + i); //$NON-NLS-1$
	    }
	    return new GroupSymbol("NEWG" + i);	         //$NON-NLS-1$
	}

    private void helpTest(LanguageObject obj, Map symbolMap) {        
        // Get old elements and groups
        List oldSymbols = (List) ElementCollectorVisitor.getElements(obj, false);
        GroupCollectorVisitor.getGroups(obj, oldSymbols);
        
        // Run symbol mapper
        StaticSymbolMappingVisitor visitor = new StaticSymbolMappingVisitor(symbolMap);
        DeepPreOrderNavigator.doVisit(obj, visitor);
        
        // Get new elements and groups
        List newSymbols = (List) ElementCollectorVisitor.getElements(obj, false);
        GroupCollectorVisitor.getGroups(obj, newSymbols);        
        
        // Check number of elements and groups
        assertEquals("Different number of symbols after mapping: ", oldSymbols.size(), newSymbols.size()); //$NON-NLS-1$
        
        // Compare mapped elements
        Iterator oldIter = oldSymbols.iterator();
        Iterator newIter = newSymbols.iterator();
        while(oldIter.hasNext()) { 
        	Symbol oldSymbol = (Symbol) oldIter.next();
        	Symbol newSymbol = (Symbol) newIter.next();
        	Symbol expectedSymbol = (Symbol) symbolMap.get(oldSymbol);
       	    assertEquals("Did not get correct mapped symbol: ", expectedSymbol, newSymbol); //$NON-NLS-1$
        }        
    }    
        
	// ################################## ACTUAL TESTS ################################
	
	public void testVisitCompareCriteria() { 
		CompareCriteria cc = new CompareCriteria(exampleElement(true, 0), CompareCriteria.EQ, exampleElement(true, 1));
		helpTest(cc, getSymbolMap());
	}
	
	public void testVisitDelete1() {
		Delete delete = new Delete(exampleGroup(true, 0));
		helpTest(delete, getSymbolMap());
	}
	
	public void testVisitDelete2() {
		Delete delete = new Delete(exampleGroup(true, 0));
		delete.setCriteria(new CompareCriteria(exampleElement(true, 0), CompareCriteria.EQ, exampleElement(true, 1)));
		helpTest(delete, getSymbolMap());
	}

	public void testVisitGroupBy() { 
	    GroupBy gb = new GroupBy();
	    gb.addSymbol(exampleElement(true, 0));
	    gb.addSymbol(exampleElement(true, 1));
	    helpTest(gb, getSymbolMap());
	}
	
	public void testVisitInsert1() { 
		Insert insert = new Insert();
		insert.setGroup(exampleGroup(true, 0));
		List vars = new ArrayList();
		vars.add(exampleElement(true, 0));
		vars.add(exampleElement(true, 1));
		insert.setVariables(vars);    
		List values = new ArrayList();
		values.add(new Constant("abc")); //$NON-NLS-1$
		values.add(new Constant("abc")); //$NON-NLS-1$
		insert.setValues(values);		
		helpTest(insert, getSymbolMap());
	}

	public void testVisitInsert2() { 
		Insert insert = new Insert();
		insert.setGroup(exampleGroup(true, 0));
		List values = new ArrayList();
		values.add(new Constant("abc")); //$NON-NLS-1$
		values.add(new Constant("abc")); //$NON-NLS-1$
		insert.setValues(values);		
		helpTest(insert, getSymbolMap());
	}
	
	public void testVisitIsNullCriteria() { 
		IsNullCriteria inc = new IsNullCriteria(exampleElement(true, 0));
		helpTest(inc, getSymbolMap());
	}
	
    public void testVisitMatchCriteria() { 
        MatchCriteria mc = new MatchCriteria(exampleElement(true, 0), new Constant("abc")); //$NON-NLS-1$
        helpTest(mc, getSymbolMap());       
    }
    
    public void testVisitBetweenCriteria() { 
        BetweenCriteria bc = new BetweenCriteria(exampleElement(true, 0), new Constant(new Integer(1000)), new Constant(new Integer(2000)));
        helpTest(bc, getSymbolMap());       
    }
    
	public void testVisitOrderBy() { 
	    OrderBy ob = new OrderBy();
	    ob.addVariable(exampleElement(true, 0));
	    ob.addVariable(exampleElement(true, 1));
	    ob.addVariable(new AliasSymbol("abc", exampleElement(true, 2))); //$NON-NLS-1$
	    helpTest(ob, getSymbolMap());	    
	}
	
	public void testVisitSelect1() { 
		Select select = new Select();
		helpTest(select, getSymbolMap());   
	}

	public void testVisitSelect2() { 
		Select select = new Select();
		MultipleElementSymbol all = new MultipleElementSymbol();
		select.addSymbol(all);
		helpTest(select, getSymbolMap());   
	}

	public void testVisitSelect3() { 
		Select select = new Select();
		MultipleElementSymbol all = new MultipleElementSymbol();
		all.addElementSymbol(exampleElement(true, 0));
		select.addSymbol(all);
		helpTest(select, getSymbolMap());   
	}
	
	public void testVisitSelect4() { 
		Select select = new Select();
		select.addSymbol( new ExpressionSymbol(
			"x", new Function("length", new Expression[] {exampleElement(true, 0)})) );    //$NON-NLS-1$ //$NON-NLS-2$
		select.addSymbol( new MultipleElementSymbol("abc.*") ); //$NON-NLS-1$
		select.addSymbol( exampleElement(true, 1) );
		helpTest(select,getSymbolMap());
	}

    public void testVisitSubquerySetCriteria() {
        SubquerySetCriteria ssc = new SubquerySetCriteria();
        ssc.setExpression(new Function("length", new Expression[] {exampleElement(true, 0)})); //$NON-NLS-1$
        ssc.setCommand(new Query());
        helpTest(ssc,getSymbolMap());
    }
	
	public void testVisitUnaryFromClause() { 
		UnaryFromClause ufc = new UnaryFromClause(exampleGroup(true, 0));
		helpTest(ufc, getSymbolMap());    
	}
	
	public void testVisitUpdate1() { 
		Update update = new Update();
		update.setGroup(exampleGroup(true, 0));
		update.addChange(exampleElement(true, 0), new Constant("abc"));    //$NON-NLS-1$
		update.addChange(exampleElement(true, 1), new Constant("abc"));    //$NON-NLS-1$
		helpTest(update, getSymbolMap());
	}

	public void testVisitUpdate2() { 
		Update update = new Update();
		update.setGroup(exampleGroup(true, 0));
		update.addChange(exampleElement(true, 0), new Constant("abc"));    //$NON-NLS-1$
		update.addChange(exampleElement(true, 1), new Constant("abc"));    //$NON-NLS-1$
		update.setCriteria(new CompareCriteria(exampleElement(true, 2), CompareCriteria.LT, new Constant("xyz"))); //$NON-NLS-1$
		helpTest(update, getSymbolMap());
	}

	public void testVisitAliasSymbol() {
		AliasSymbol as = new AliasSymbol("abc", exampleElement(true, 0)); //$NON-NLS-1$
		helpTest(as, getSymbolMap());
	}
	
 	public void testVisitAllSymbol() {
 		MultipleElementSymbol as = new MultipleElementSymbol();
 		ArrayList elements = new ArrayList();
 		elements.add(exampleElement(true, 0));    
 		elements.add(exampleElement(true, 1));     		
 		as.setElementSymbols(elements);
 		helpTest(as, getSymbolMap());
 	}
 	
 	public void testVisitMultipleElementSymbol() {
 		MultipleElementSymbol aigs = new MultipleElementSymbol("OLDG0.*"); //$NON-NLS-1$
 		ArrayList elements = new ArrayList();
 		elements.add(exampleElement(true, 0));    
 		elements.add(exampleElement(true, 1));     		
 		aigs.setElementSymbols(elements);
 		helpTest(aigs, getSymbolMap()); 	    
 	}
 	
 	public void testFunction1() {
 	    Function f = new Function("concat", new Expression[] {}); //$NON-NLS-1$
		helpTest(f, getSymbolMap()); 	    
 	} 	

 	public void testFunction2() {
 	    Function f = new Function("concat", new Expression[] {exampleElement(true, 0), exampleElement(true, 1) }); //$NON-NLS-1$
		helpTest(f, getSymbolMap()); 	    
 	} 	

 	public void testFunction3() {
 	    Function f1 = new Function("concat", new Expression[] {exampleElement(true, 0), exampleElement(true, 1) }); //$NON-NLS-1$
 	    Function f2 = new Function("length", new Expression[] { f1 }); //$NON-NLS-1$
		helpTest(f2, getSymbolMap()); 	    
 	} 	

 	public void testMapMultipleElementSymbolName() {
 		MultipleElementSymbol aigs = new MultipleElementSymbol("OLDG0"); //$NON-NLS-1$
 		ArrayList<ElementSymbol> elements = new ArrayList<ElementSymbol>();
 		elements.add(exampleElement(true, 0));    
 		elements.add(exampleElement(true, 1));     		
 		aigs.setElementSymbols(elements);
 		
        // Run symbol mapper
        StaticSymbolMappingVisitor visitor = new StaticSymbolMappingVisitor(getSymbolMap());
        DeepPreOrderNavigator.doVisit(aigs, visitor);

		// Check name of all in group symbol
		assertEquals("MultipleElementSymbol name did not get mapped correctly: ", "NEWG0.*", aigs.toString()); //$NON-NLS-1$ //$NON-NLS-2$
 	}
    
    public void testExecName() {
        StoredProcedure exec = new StoredProcedure();
        exec.setProcedureName(exampleGroup(true, 1).getName());
        exec.setProcedureID("proc"); //$NON-NLS-1$

        // Run symbol mapper
        StaticSymbolMappingVisitor visitor = new StaticSymbolMappingVisitor(getSymbolMap());
        DeepPreOrderNavigator.doVisit(exec, visitor);

        // Check that group got mapped
        assertEquals("Procedure name did not get mapped correctly: ", exampleGroup(false, 1).getName(), exec.getProcedureName()); //$NON-NLS-1$

    }    
    
    public void testExecParamElement() {
        StoredProcedure exec = new StoredProcedure();
        exec.setProcedureName("pm1.proc1"); //$NON-NLS-1$
        exec.setProcedureID("proc"); //$NON-NLS-1$
        SPParameter param1 = new SPParameter(1, exampleElement(true, 1));
        exec.setParameter(param1);
        
        // Run symbol mapper
        StaticSymbolMappingVisitor visitor = new StaticSymbolMappingVisitor(getSymbolMap());
        DeepPreOrderNavigator.doVisit(exec, visitor);
        
        // Check that element got switched
        assertEquals("Stored proc param did not get mapped correctly: ", exampleElement(false, 1), param1.getExpression());    //$NON-NLS-1$
    }

    public void testExecParamFunction() {
        StoredProcedure exec = new StoredProcedure();
        exec.setProcedureName("pm1.proc1"); //$NON-NLS-1$
        exec.setProcedureID("proc"); //$NON-NLS-1$
        Function f = new Function("length", new Expression[] { exampleElement(true, 1) }); //$NON-NLS-1$
        
        SPParameter param1 = new SPParameter(1, f);
        exec.setParameter(param1);

        // Run symbol mapper
        StaticSymbolMappingVisitor visitor = new StaticSymbolMappingVisitor(getSymbolMap());
        DeepPreOrderNavigator.doVisit(exec, visitor);

        // Check that element got switched
        Function afterFunc = (Function) param1.getExpression();
        assertEquals("Stored proc param did not get mapped correctly: ", exampleElement(false, 1), afterFunc.getArg(0)); //$NON-NLS-1$
    }

    public void testExecParamNestedFunction() {
        StoredProcedure exec = new StoredProcedure();
        exec.setProcedureName("pm1.proc1"); //$NON-NLS-1$
        exec.setProcedureID("proc"); //$NON-NLS-1$        
        Function f = new Function("length", new Expression[] { exampleElement(true, 1) }); //$NON-NLS-1$
        Function f2 = new Function("+", new Expression[] { f, new Constant(new Integer(1)) });  //$NON-NLS-1$
        
        SPParameter param1 = new SPParameter(1, f2);
        exec.setParameter(param1);

        // Run symbol mapper
        StaticSymbolMappingVisitor visitor = new StaticSymbolMappingVisitor(getSymbolMap());
        DeepPreOrderNavigator.doVisit(exec, visitor);

        // Check that element got switched
        Function afterFunc = (Function) param1.getExpression();
        Function innerFunc = (Function) afterFunc.getArgs()[0];
        assertEquals("Stored proc param did not get mapped correctly: ", exampleElement(false, 1), innerFunc.getArg(0)); //$NON-NLS-1$
    }

    public void testCriteriaSelector() {
        CriteriaSelector selector = new CriteriaSelector();
        selector.setSelectorType(CriteriaSelector.COMPARE_EQ);
        selector.addElement(exampleElement(true, 1));
        
        // Run symbol mapper
        StaticSymbolMappingVisitor visitor = new StaticSymbolMappingVisitor(getSymbolMap());
        DeepPreOrderNavigator.doVisit(selector, visitor);
        
        // Check that element got mapped
        assertEquals("Criteria selector element did not get mapped correctly: ", exampleElement(false, 1), selector.getElements().get(0)); //$NON-NLS-1$
    }
    
    public void testTranslateCriteria() {
        TranslateCriteria trans = new TranslateCriteria();
        trans.addTranslation(new CompareCriteria(exampleElement(false, 1), CompareCriteria.EQ, exampleElement(false, 2)));

        // Run symbol mapper
        StaticSymbolMappingVisitor visitor = new StaticSymbolMappingVisitor(getSymbolMap());
        DeepPreOrderNavigator.doVisit(trans, visitor);

        // Check that element got mapped
        List elements = (List) ElementCollectorVisitor.getElements(trans, false);
        assertEquals("Translate criteria element did not get mapped correctly: ", exampleElement(false, 1), elements.get(0)); //$NON-NLS-1$
        assertEquals("Translate criteria element did not get mapped correctly: ", exampleElement(false, 2), elements.get(1)); //$NON-NLS-1$
            
    }
}
