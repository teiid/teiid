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

package org.teiid.coherence.translator;

import java.util.*;

import org.teiid.language.*;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;



/**
 */
public class CoherenceVisitor extends HierarchyVisitor {

	private String tableName = null;
	private List<Long> ids = new ArrayList<Long>();
	private RuntimeMetadata metadata;

    private TranslatorException exception;

    /**
     * 
     */
    public CoherenceVisitor(RuntimeMetadata metadata) {
        super();        
        this.metadata = metadata;
    }


    public Map<String, List<Long>> getCriteria() {
    	
    	Map<String, List<Long>> criteria = new HashMap<String, List<Long>>();
    	criteria.put(tableName, ids);
    	
        return criteria;
    }
    
    public TranslatorException getException() {
        return this.exception;
    }

    
	public void visit(Select query) {
		super.visit(query);
		List<TableReference> tables = query.getFrom();
		TableReference t = tables.get(0);
		if(t instanceof NamedTable) {
			Table group = ((NamedTable)t).getMetadataObject();
			tableName = group.getName();
		}
		
	}
	
	
    public void visit(Comparison obj) {
        Expression expr = obj.getRightExpression();
        addIdFromExpression(expr);        
    }

    public void visit(In obj) {
        List exprs = obj.getRightExpressions();
        Iterator iter = exprs.iterator();
        while(iter.hasNext()) {
            Expression expr = (Expression) iter.next();
            addIdFromExpression(expr);
            
        }
    }
    
    private void addIdFromExpression(Expression expr) {
        if(expr instanceof Literal) {
        	Long longparm = null;
            Literal literal = (Literal) expr;
            if(literal.getType() == String.class) {
                String parm = (String) literal.getValue();
                
                longparm = new Long(parm);
                
            } else if (literal.getType() == Long.class) {
            	Object obj = literal.getValue();
            	
                longparm = ((Long)obj);

                
            } else {
                this.exception = new TranslatorException("CoherenceVisitor.Unexpected_type", literal.getType().getName()); //$NON-NLS-1$
            }
            
            ids.add(longparm);
  
        } else {
            this.exception = new TranslatorException("TickerCollectorVisitor.Unexpected_expression" + expr); //$NON-NLS-1$
        }
         
    }

}
