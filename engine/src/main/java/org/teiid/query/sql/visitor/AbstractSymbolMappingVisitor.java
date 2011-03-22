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
import java.util.Iterator;
import java.util.List;

import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.proc.CriteriaSelector;
import org.teiid.query.sql.symbol.AllInGroupSymbol;
import org.teiid.query.sql.symbol.AllSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;


/**
 * <p> This class is used to update LanguageObjects by replacing one set of symbols with
 * another.  There is one abstract method which must be overridden to define how the 
 * mapping lookup occurs.</p>
 */
public abstract class AbstractSymbolMappingVisitor extends ExpressionMappingVisitor {
	
	private List unmappedSymbols;
	
    protected AbstractSymbolMappingVisitor() {
        super(null);
    }

    /**
     * Get the mapped symbol from the specified symbol.  Subclasses should implement
     * this method to look up the target symbol from the specified symbol.
     * @param symbol Source symbol
     * @return Target symbol
     */
    protected abstract Symbol getMappedSymbol(Symbol symbol);
            
    // ############### Visitor methods for language objects ##################

    /**
     * <p> This method updates the <code>Insert</code> object it receives as an
     * argument by replacing the virtual groups/elements with their physical
     * counterparts.</p>
     * @param obj The Insert object to be updated with physical groups/elements
     */
    public void visit(Insert obj) {
    	
    	List physicalElements = new ArrayList();
    	
    	// get the GroupSymbol on the insert
    	GroupSymbol virtualGroup = obj.getGroup();
    	obj.setGroup(getMappedGroup(virtualGroup));
    	
    	// get all virtual columns present on the Insert and  replace them with
    	// physical elements
    	if(obj.getVariables() != null) { 
        	Iterator elementIter = obj.getVariables().iterator();
        	while(elementIter.hasNext()) {
        		ElementSymbol virtualElement = (ElementSymbol) elementIter.next();
        		physicalElements.add(getMappedElement(virtualElement));
        	}
        	obj.setVariables(physicalElements);
    	}
    }
    
    /**
     * <p> This method updates the <code>Delete</code> object it receives as an
     * argument by replacing the virtual groups/elements with their physical
     * counterparts.</p>
     * @param obj The Delete object to be updated with physical groups
     */
    public void visit(Delete obj) {
    	
    	// get the GroupSymbol on the delete
    	GroupSymbol virtualGroup = obj.getGroup();
    	obj.setGroup(getMappedGroup(virtualGroup));    	
    }
    
    /**
     * <p> This method updates the <code>Update</code> object it receives as an
     * argument by replacing the virtual groups/elements with their physical
     * counterparts.</p>
     * @param obj The Update object to be updated with physical groups
     */
    public void visit(Update obj) {
    	
    	// get the GroupSymbol on the update
    	GroupSymbol virtualGroup = obj.getGroup();
    	obj.setGroup(getMappedGroup(virtualGroup));
    }
    
    public void visit(SetClause obj) {
    	obj.setSymbol(getMappedElement(obj.getSymbol()));
    }
    
	/**
	 * Swap each ElementSymbol referenced by AllInGroupSymbol
	 * @param obj Object to remap
	 */
    public void visit(AllInGroupSymbol obj) {   
        // Discover new group name during course of mapping
        String newGroupName = null;
                     
		List oldSymbols = obj.getElementSymbols();
		if(oldSymbols != null && oldSymbols.size() > 0) {
			List newSymbols = new ArrayList(oldSymbols.size());
			
			Iterator iter = oldSymbols.iterator();
			while(iter.hasNext()) {
				ElementSymbol es = (ElementSymbol) iter.next();    
				ElementSymbol mappedSymbol = getMappedElement(es);
				
				// Save group name on first valid mapped element
				if(newGroupName == null && mappedSymbol != null) { 
				    GroupSymbol newGroup = mappedSymbol.getGroupSymbol();
				    if(newGroup != null) { 
					    newGroupName = newGroup.getName();
				    }
				}
				
				newSymbols.add( mappedSymbol );
			}
			obj.setElementSymbols(newSymbols);
		} 	

		// If haven't discovered group name yet (if, for instance, stuff isn't resolved),
		// then fake up a group symbol, map it, and use the name of the mapped group symbol
		if(newGroupName == null) {
			String symbolName = obj.getName();
			String oldGroupName = symbolName.substring(0, symbolName.length()-2);	// cut .* off
			
			GroupSymbol fakeSymbol = new GroupSymbol(oldGroupName);
			GroupSymbol mappedSymbol = getMappedGroup(fakeSymbol);
			
			newGroupName = mappedSymbol.getName();
		}
		
		// Finally, swap name of group, which should be the name of the group
		// for all of the element symbols
		obj.setShortName(newGroupName + ".*"); //$NON-NLS-1$
			
    }

	/**
	 * Swap each ElementSymbol referenced by AllSymbol
	 * @param obj Object to remap
	 */
    public void visit(AllSymbol obj) {
		List oldSymbols = obj.getElementSymbols();
		if(oldSymbols != null && oldSymbols.size() > 0) {
			List newSymbols = new ArrayList(oldSymbols.size());
			
			Iterator iter = oldSymbols.iterator();
			while(iter.hasNext()) {
				ElementSymbol es = (ElementSymbol) iter.next();    
				newSymbols.add( getMappedElement(es) );
			}

			obj.setElementSymbols(newSymbols);
		}		
    }
    
	/**
	 * Swap group in unary from clause.
	 * @param obj Object to remap
	 */
    public void visit(UnaryFromClause obj) {
    	GroupSymbol srcGroup = obj.getGroup();
    	obj.setGroup(getMappedGroup(srcGroup));        
    }

    /**
     * Swap name of stored proc and elements in stored procedure parameter expressions
     * @param obj Object to remap
     */
    public void visit(StoredProcedure obj) {
        // Swap procedure name
        String execName = obj.getProcedureName();
        GroupSymbol fakeGroup = new GroupSymbol(execName);
        Object procedureID = obj.getProcedureID();
        if(procedureID != null) {
            fakeGroup.setMetadataID(procedureID);
        }
        GroupSymbol mappedGroup = getMappedGroup(fakeGroup);
        obj.setProcedureName(mappedGroup.getName());
        
        super.visit(obj);
    }

    /**
     * Swap elements in CriteriaSelector
     * @param obj Object to remap
     */
    public void visit(CriteriaSelector obj) {
        CriteriaSelector selector = obj;
        if(selector.hasElements()) {
            // Map each element and reset
            List elements = selector.getElements();
            List mappedElements = new ArrayList(elements.size());

            Iterator elemIter = elements.iterator();
            while(elemIter.hasNext()) {
                ElementSymbol elem = (ElementSymbol) elemIter.next();
                mappedElements.add(getMappedElement(elem));    
            }            
            
            selector.setElements(mappedElements);
        }    
    }
        
    /* ############### Helper Methods ##################   */
    
    /** 
     * @see org.teiid.query.sql.visitor.ExpressionMappingVisitor#replaceExpression(org.teiid.query.sql.symbol.Expression)
     */
    @Override
    public Expression replaceExpression(Expression element) {
        if (element instanceof ElementSymbol) {
            return getMappedElement((ElementSymbol)element);
        }
        return element;
    }
    
    /**
     * <p> This method looks up the symbol map for a physical <code>ElementSymbol</code>
     * given a virtual <code>ElementSymbol</code> object.</p>
     * @param obj The virtual <code>ElementSymbol</code> object whose physical counterpart is returned
     * @return The physical <code>ElementSymbol</code> object or null if the object could not be mapped
     */    
    private ElementSymbol getMappedElement(ElementSymbol obj) {
    	
    	ElementSymbol element = (ElementSymbol) getMappedSymbol(obj);   
    	
    	if(element != null) { 
    	    return element;    		    
    	}
		markUnmapped(obj);			
		return obj;    	    
    }
    
    /**
     * <p> This method looks up the symbol map for a physical <code>GroupSymbol</code>
     * given a virtual <code>GroupSymbol</code> object.</p>
     * @param obj The virtual <code>GroupSymbol</code> object whose physical counterpart is returned
     * @return The physical <code>GroupSymbol</code> object or null if the object could not be mapped
     */    
    private GroupSymbol getMappedGroup(GroupSymbol obj) {
    	
    	GroupSymbol group = (GroupSymbol) getMappedSymbol(obj);
    	
    	if(group != null) { 
    	    return group;    		    
    	}
		markUnmapped(obj);			
		return obj;    	    
    }    
    
    /**
     * Mark an element as unmapped as no mapping could be found. 
     * @param symbol Unmapped symbol
     */
    private void markUnmapped(Symbol symbol) { 
    	if(unmappedSymbols == null) { 
    	 	unmappedSymbols = new ArrayList();   
    	}    
    	
    	unmappedSymbols.add(symbol);
    }
    
    /**
     * Get all symbols that were not mapped during life of visitor.  If all symbols
     * were mapped, this will return null.
     * @return List of ElementSymbol and GroupSymbol that were unmapped OR null if 
     * all symbols mapped successfully
     */
    public List getUnmappedSymbols() { 
    	return unmappedSymbols;    
    }

}
