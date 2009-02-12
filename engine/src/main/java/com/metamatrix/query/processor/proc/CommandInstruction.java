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

package com.metamatrix.query.processor.proc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.query.processor.program.ProgramInstruction;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.util.VariableContext;


/** 
 * This abstract base class has common methods for dealing with instructions that 
 * have commands.  In particular commands may have nested EXECs that have parameter
 * references, which require some special handling.
 * 
 * @since 4.4
 */
public abstract class CommandInstruction extends ProgramInstruction {

    private Collection references;
                 
    protected Collection getReferences() {
        return this.references;
    }
    
    public void setReferences(Collection references) {
        this.references = references;
    }

    /**
     * <p> Set the value the <code>Reference</code> objects evaluate to by looking up the
     * variable context for the value of the <code>ElementSymbol</code> contained in the reference.
     * @param references List containing References whose values are to be set.
     * @param varContext The variableContext to be looked up for the value
     * @throws MetaMatrixComponentException if the value for the refwerence could not be found.
     */
    void setReferenceValues(VariableContext varContext) throws MetaMatrixComponentException {
        setReferenceValues(varContext, references);
    }
    
    static void setReferenceValues(VariableContext varContext, Collection references)
                 throws MetaMatrixComponentException {
        
        Iterator refIter = references.iterator();
        while(refIter.hasNext()) {
            Reference ref = (Reference) refIter.next();
            Expression expr = ref.getExpression();
            if(expr instanceof ElementSymbol) {
                ElementSymbol elmnt = (ElementSymbol) expr;
                if(varContext.containsVariable(elmnt)) {
                    ref.setValue(varContext.getValue(elmnt));
                }
            }
        }
    }

    protected List cloneReferences() {
        List copyReferences = new ArrayList(references.size());
        Iterator iter = references.iterator();
        while(iter.hasNext()) {
            Reference ref = (Reference) iter.next();
            copyReferences.add(ref.clone());
        }
        return copyReferences;
    }

}
