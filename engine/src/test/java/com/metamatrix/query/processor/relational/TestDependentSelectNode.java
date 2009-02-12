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

package com.metamatrix.query.processor.relational;

import java.util.*;

import junit.framework.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.*;
import com.metamatrix.query.processor.*;
import com.metamatrix.query.sql.lang.*;
import com.metamatrix.query.sql.symbol.*;
import com.metamatrix.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import com.metamatrix.query.util.CommandContext;

/**
 * Tests {@link DependentSelectNode DependentSelectNode} class
 */
public class TestDependentSelectNode extends TestCase {

    /**
     * Constructor for TestDependentSelectNode.
     * @param name
     */
    public TestDependentSelectNode(String name) {
        super(name);
    }

	//Just for testing clone() method of DependentSelectNode
	private static class DoNothingProcessorPlan implements ProcessorPlan{ 
		private DoNothingProcessorPlan original;
		public DoNothingProcessorPlan(){}
		public DoNothingProcessorPlan(DoNothingProcessorPlan original){
			this.original = original;
		}
		public Object clone() {return new DoNothingProcessorPlan(this);}
		public void close() throws MetaMatrixComponentException {}
		public void connectTupleSource(TupleSource source, int dataRequestID) {}
		public List getAndClearWarnings() {return null;}
		public List getOutputElements() {return null;}
		public void initialize(
            CommandContext context,
			ProcessorDataManager dataMgr,
			BufferManager bufferMgr) {}
        public CommandContext getContext() { return null; }
        public TupleBatch nextBatch()throws BlockedException, MetaMatrixComponentException{return null;}
		public void open() throws MetaMatrixComponentException {}
		public void reset() {}
        /* (non-Javadoc)
         * @see com.metamatrix.query.processor.ProcessorPlan#getUpdateCount()
         */
        public int getUpdateCount() {
            // TODO Auto-generated method stub
            return 0;
        }
        public Map getDescriptionProperties() {
            return new HashMap();
        }
        public Collection getChildPlans() {
            return Collections.EMPTY_LIST;
        }
        
	}

	private List helpMakeFakeProcessorPlans(int count){
		List result = new ArrayList(count);
		for (int i=0; i<count; i++){
			result.add(new DoNothingProcessorPlan());
		}
		return result;
	}
	
	private Command helpMakeCommand(){
		Select select = new Select();
		ElementSymbol element = new ElementSymbol("e2"); //$NON-NLS-1$
		select.addSymbol(element);
		From from = new From();
		GroupSymbol pm1g2 = new GroupSymbol("pm1.g2"); //$NON-NLS-1$
		from.addGroup(pm1g2);
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		return query;
	}

	private Command helpMakeCommand2(){
		Select select = new Select();
		ElementSymbol element = new ElementSymbol("e3"); //$NON-NLS-1$
		select.addSymbol(element);
		From from = new From();
		GroupSymbol pm1g2 = new GroupSymbol("pm4.g5"); //$NON-NLS-1$
		from.addGroup(pm1g2);
		Query query = new Query();
		query.setSelect(select);
		query.setFrom(from);
		return query;
	}

	/**
	 * Tests clone() method of DependentSelectNode
	 */
	public void testClone(){
		int id = 1;
		DependentSelectNode node = new DependentSelectNode(id);
		Command subCommand = helpMakeCommand();
		Criteria subCrit = new SubquerySetCriteria(new Constant("3"), subCommand); //$NON-NLS-1$
		Command subCommand2 = helpMakeCommand2();
		Criteria subCrit2 = new SubquerySetCriteria(new ElementSymbol("f"), subCommand2); //$NON-NLS-1$
		//Set up DependentSelectNode data
		List plans = helpMakeFakeProcessorPlans(2);
		List crits = new ArrayList(2);
		crits.add(subCrit);
		crits.add(subCrit2);
		node.setPlansAndCriteriaMapping(plans, crits);
		//Set up criteria
		CompoundCriteria crit = new CompoundCriteria();
		crit.addCriteria(subCrit);
		crit.addCriteria(subCrit2);
		crit.setOperator(CompoundCriteria.AND);
//		Criteria otherCrit = new IsNullCriteria(new ElementSymbol("fakeElement"));
//		crit.addCriteria(otherCrit);
		node.setCriteria(crit);
		
		//Test clone
		DependentSelectNode cloned = (DependentSelectNode)node.clone();
		
		List originalProcessorPlans = node.getSubqueryProcessorUtility().getSubqueryPlans();
		List clonedProcessorPlans = cloned.getSubqueryProcessorUtility().getSubqueryPlans();
		List clonedCrits = cloned.getSubqueryProcessorUtility().getValueIteratorProviders();
		List checkClonedCrits = new ArrayList(clonedCrits.size());
        ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(cloned.getCriteria(), checkClonedCrits);
        assertTrue(clonedProcessorPlans.size() == 2);
		assertEquals(originalProcessorPlans.size(), clonedProcessorPlans.size());
		assertEquals(clonedCrits.size(), checkClonedCrits.size());
		assertEquals(clonedCrits.size(), clonedProcessorPlans.size());
		for (int i=0; i<originalProcessorPlans.size(); i++){
			//Check ProcessorPlans
			Object originalPlan = originalProcessorPlans.get(i);
			DoNothingProcessorPlan clonedPlan = (DoNothingProcessorPlan)clonedProcessorPlans.get(i);
			assertNotNull(clonedPlan.original);
			assertSame(originalPlan, clonedPlan.original);
			
			//Check SubquerySetCriteria
			assertNotNull(clonedCrits.get(i));
			assertSame(clonedCrits.get(i), checkClonedCrits.get(i));			
		}
		
	}

    /**
     * Tests clone() method of DependentSelectNode
     */
    public void testClone2(){
        int id = 1;
        DependentSelectNode node = new DependentSelectNode(id);

        Command subCommand = helpMakeCommand();
        Criteria subCrit = new SubqueryCompareCriteria(new ElementSymbol("f"), subCommand, SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ANY); //$NON-NLS-1$

        Command subCommand2 = helpMakeCommand2();
        Criteria subCrit2 = new ExistsCriteria(subCommand2);

        //Set up DependentSelectNode data
        List plans = helpMakeFakeProcessorPlans(2);
        List crits = new ArrayList(2);
        crits.add(subCrit);
        crits.add(subCrit2);
        node.setPlansAndCriteriaMapping(plans, crits);
        //Set up criteria
        CompoundCriteria crit = new CompoundCriteria();
        crit.addCriteria(subCrit);
        crit.addCriteria(subCrit2);
        crit.setOperator(CompoundCriteria.AND);
//      Criteria otherCrit = new IsNullCriteria(new ElementSymbol("fakeElement"));
//      crit.addCriteria(otherCrit);
        node.setCriteria(crit);
        
        //Test clone
        DependentSelectNode cloned = (DependentSelectNode)node.clone();
        
        List originalProcessorPlans = node.getSubqueryProcessorUtility().getSubqueryPlans();
        List clonedProcessorPlans = cloned.getSubqueryProcessorUtility().getSubqueryPlans();
        List clonedCrits = cloned.getSubqueryProcessorUtility().getValueIteratorProviders();
        List checkClonedCrits = new ArrayList(clonedCrits.size());
        ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(cloned.getCriteria(), checkClonedCrits);
        assertTrue(clonedProcessorPlans.size() == 2);
        assertEquals(originalProcessorPlans.size(), clonedProcessorPlans.size());
        assertEquals(clonedCrits.size(), checkClonedCrits.size());
        assertEquals(clonedCrits.size(), clonedProcessorPlans.size());
        for (int i=0; i<originalProcessorPlans.size(); i++){
            //Check ProcessorPlans
            Object originalPlan = originalProcessorPlans.get(i);
            DoNothingProcessorPlan clonedPlan = (DoNothingProcessorPlan)clonedProcessorPlans.get(i);
            assertNotNull(clonedPlan.original);
            assertSame(originalPlan, clonedPlan.original);
            
            //Check SubquerySetCriteria
            assertNotNull(clonedCrits.get(i));
            assertSame(clonedCrits.get(i), checkClonedCrits.get(i));            
        }
    }
    
}
