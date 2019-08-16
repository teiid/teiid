/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.Symbol;


/**
 * <p> This class is used to update LanguageObjects by replacing one set of symbols with
 * another.  There is one abstract method which must be overridden to define how the
 * mapping lookup occurs.
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
     * counterparts.
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
     * counterparts.
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
     * counterparts.
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
    public void visit(MultipleElementSymbol obj) {
        List<ElementSymbol> oldSymbols = obj.getElementSymbols();
        if(oldSymbols != null && oldSymbols.size() > 0) {
            List<ElementSymbol> newSymbols = new ArrayList<ElementSymbol>(oldSymbols.size());

            Iterator<ElementSymbol> iter = oldSymbols.iterator();
            while(iter.hasNext()) {
                ElementSymbol es = iter.next();
                ElementSymbol mappedSymbol = getMappedElement(es);
                newSymbols.add( mappedSymbol );
            }
            obj.setElementSymbols(newSymbols);
        }

        if (obj.getGroup() == null) {
            return;
        }

        obj.setGroup(getMappedGroup(obj.getGroup()));
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
     * given a virtual <code>ElementSymbol</code> object.
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
     * given a virtual <code>GroupSymbol</code> object.
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
