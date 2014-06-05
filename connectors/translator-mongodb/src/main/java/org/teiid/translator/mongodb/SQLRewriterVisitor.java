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
package org.teiid.translator.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.*;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.translator.TranslatorException;

public class SQLRewriterVisitor {
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    ArrayList<HierarchyVisitor> rewriters = new ArrayList<HierarchyVisitor>();
    
    public static void rewrite(Select obj) throws TranslatorException {
        new SQLRewriterVisitor(obj);
    }
    
    private SQLRewriterVisitor(Select obj) throws TranslatorException {
        this.rewriters.add(new CountElementRewriter());
        
        for (HierarchyVisitor visitor:this.rewriters) {
            visitor.visitNode(obj);
        }
        
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }
    }
    
    /**
     * rewrites
     * select count (name) from customer
     * to
     * select count(*) from customer where name is not null
     */
    public class CountElementRewriter extends HierarchyVisitor {
        @Override
        public void visit(Select obj) {
            if (!isAppplicable(obj)) {
                return;
            }
            
            if (!isSupported(obj)) {
                return;
            }
            
            List<DerivedColumn> columns = new ArrayList(obj.getDerivedColumns());
            obj.getDerivedColumns().clear();
            
            for (DerivedColumn column:columns) {
               if (column.getExpression() instanceof AggregateFunction) {
                   AggregateFunction agg = (AggregateFunction)column.getExpression();
                   if (agg.getName().equals(AggregateFunction.COUNT) && !agg.getParameters().isEmpty()) {
                                  
                       // now add criteria on parameter
                       Condition crit = new IsNull(agg.getParameters().get(0), true); 
                       if (obj.getWhere() != null) {
                           obj.setWhere(new AndOr(obj.getWhere(), crit, AndOr.Operator.AND));
                       }
                       else {
                           obj.setWhere(crit);
                       }
                       
                       // redefine the aggregate to count(*)
                       agg = LanguageFactory.INSTANCE.createAggregate(AggregateFunction.COUNT, false, null, Integer.class); 
                       column.setExpression(agg);
                   }               
               }
               obj.getDerivedColumns().add(column);
            }
        }        
        
        private boolean isSupported(Select obj) {
            int applicable = 0;
            for (DerivedColumn column:obj.getDerivedColumns()) {
                if (column.getExpression() instanceof AggregateFunction) {
                    applicable++;
                }
            }
            if (applicable > 1) {
                exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18024, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18024)));
            }

            return (applicable <= 1);            
        }
        
        private boolean isAppplicable(Select obj) {
            for (DerivedColumn column:obj.getDerivedColumns()) {
                if (column.getExpression() instanceof AggregateFunction) {
                    AggregateFunction agg = (AggregateFunction)column.getExpression();
                    if (agg.getName().equals(AggregateFunction.COUNT) && !agg.getParameters().isEmpty()) {                        
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
