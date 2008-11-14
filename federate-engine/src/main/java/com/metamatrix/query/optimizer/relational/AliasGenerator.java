/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.optimizer.relational;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.ExpressionSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;

/**
 * Adds safe (generated) aliases to the source command
 */
public class AliasGenerator extends PreOrderNavigator {
    
    private static class SQLNamingContext {
        SQLNamingContext parent;
        
        Map<String, Map<String, String>> elementMap = new HashMap<String, Map<String, String>>();
        Map<String, String> groupNames = new HashMap<String, String>();
        Map<String, String> currentSymbolNames = new HashMap<String, String>();
        
        boolean aliasColumns = false;
        
        public SQLNamingContext(SQLNamingContext parent) {
            this.parent = parent;
        }
        
        public String getElementName(String group, String name) {
            if (group == null) {
                return currentSymbolNames.get(name.toUpperCase());
            }
            Map<String, String> elements = this.elementMap.get(group);
            if (elements == null) {
                if (parent == null) {
                    return null;
                }
                return parent.getElementName(group, name);
            }
            return elements.get(name);
        }
        
        public void updateElementMap(String group) {
            this.parent.elementMap.put(group.toUpperCase(), currentSymbolNames);
        }
        
        public String getGroupName(String group) {
            String groupName = groupNames.get(group);
            if (groupName == null) {
                if (parent == null) {
                    return null;
                }
                return parent.getGroupName(group);
            }
            return groupName;
        }
    }
    
    private static class NamingVisitor extends LanguageVisitor {
        
        private SQLNamingContext namingContext = new SQLNamingContext(null);
        private boolean aliasGroups;
        
        public NamingVisitor(boolean aliasGroups) {
            this.aliasGroups = aliasGroups;
        }
        
        public void visit(Reference obj) {
            if (obj.getExpression() instanceof ElementSymbol) {
                visit((ElementSymbol)obj.getExpression());
            }
        }
        
        /** 
         * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.symbol.ElementSymbol)
         */
        @Override
        public void visit(ElementSymbol obj) {
            GroupSymbol group = obj.getGroupSymbol();
            if(group == null) {
                return;
            }
            String elemShortName = obj.getShortCanonicalName();
            
            visit(group);
            
            String newName = namingContext.getElementName(group.getCanonicalName(), elemShortName);
            
            if (newName == null) {
                newName = ElementSymbol.getShortName(obj.getOutputName());
            }
            
            obj.setOutputName(group.getOutputName() + ElementSymbol.SEPARATOR + newName);
            obj.setDisplayMode(ElementSymbol.DisplayMode.OUTPUT_NAME);
        }
        
        /** 
         * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.symbol.GroupSymbol)
         */
        @Override
        public void visit(GroupSymbol obj) {
            if (aliasGroups) {
                String definition = obj.getNonCorrelationName();
                String newAlias = this.namingContext.getGroupName(obj.getCanonicalName());
                if (newAlias == null) {
                    return;
                }
                obj.setOutputName(newAlias);
                obj.setOutputDefinition(definition);
            } else if(obj.getDefinition() != null) {
                obj.setOutputName(obj.getDefinition());
                obj.setOutputDefinition(null);
            }
        }
        
        public void createChildNamingContext(boolean aliasColumns) {
            this.namingContext = new SQLNamingContext(this.namingContext);
            this.namingContext.aliasColumns = aliasColumns;
        }
        
        public void removeChildNamingContext() {
            this.namingContext = this.namingContext.parent;
        }

    }
    
    private NamingVisitor visitor;
    private int groupIndex = 0;
    private int viewIndex = 0;

    public AliasGenerator(boolean aliasGroups) {
        super(new NamingVisitor(aliasGroups));
        this.visitor = (NamingVisitor)this.getVisitor();
    }

    /**
     * visit the branches other than the first with individual naming contexts
     * Aliases are being added in all cases, even though they may only be needed in the order by case.
     * Adding the same alias to all branches ensures cross db support (db2 in particular)
     */
    public void visit(SetQuery obj) {
        visitor.createChildNamingContext(true);
        visitNode(obj.getRightQuery());
        visitor.removeChildNamingContext();
        visitor.namingContext.aliasColumns = true;
        visitNode(obj.getLeftQuery());
        visitNode(obj.getOrderBy());
    }
    
    public void visit(Select obj) {
        List selectSymbols = obj.getSymbols();
                        
        for (int i = 0; i < selectSymbols.size(); i++) {
            SingleElementSymbol symbol = (SingleElementSymbol)selectSymbols.get(i);
            
            String newAlias = "c_" + i; //$NON-NLS-1$
            
            boolean needsAlias = true;
            
            Expression expr = SymbolMap.getExpression(symbol);
            
            SingleElementSymbol newSymbol = symbol;
            
            if (!(expr instanceof SingleElementSymbol)) {
                newSymbol = new ExpressionSymbol(newSymbol.getShortName(), expr);
            } else if (expr instanceof ElementSymbol) {
                if (!needsAlias(newAlias, (ElementSymbol)expr)) {
                    needsAlias = false;
                    ((ElementSymbol)expr).setOutputName(newAlias);
                }
                newSymbol = (ElementSymbol)expr;
            } else {
                newSymbol = (SingleElementSymbol)expr; 
            }
                        
            visitor.namingContext.currentSymbolNames.put(symbol.getShortCanonicalName(), newAlias);
            if (visitor.namingContext.aliasColumns && needsAlias) {
                newSymbol = new AliasSymbol(symbol.getShortName(), newSymbol);
                newSymbol.setOutputName(newAlias);
            } 
            selectSymbols.set(i, newSymbol);
        }
        
        super.visit(obj);
    }

    private boolean needsAlias(String newAlias,
                               ElementSymbol symbol) {
        return !(symbol.getMetadataID() instanceof TempMetadataID) || !newAlias.equalsIgnoreCase(visitor.namingContext.getElementName(symbol.getGroupSymbol().getCanonicalName(), symbol.getShortCanonicalName()));
    }
    
    /**
     * visit the query in definition order
     */
    public void visit(Query obj) {
        if (obj.getOrderBy() != null) {
            visitor.namingContext.aliasColumns = true;
        }        
        visitNode(obj.getFrom());
        visitNode(obj.getCriteria());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getSelect());
        visitNode(obj.getOrderBy());
    }
    
    public void visit(SubqueryFromClause obj) {
        visitor.createChildNamingContext(true);
        obj.getCommand().acceptVisitor(this);
        visitor.namingContext.updateElementMap(obj.getName().toUpperCase());
        visitor.removeChildNamingContext();
        obj.getGroupSymbol().setOutputName(recontextGroup(obj.getGroupSymbol(), true));
    }
    
    @Override
    public void visit(UnaryFromClause obj) {
        if (visitor.aliasGroups) {
            GroupSymbol symbol = obj.getGroup();
            recontextGroup(symbol, false);
        } 
        super.visit(obj);
    }

    /** 
     * @param symbol
     */
    private String recontextGroup(GroupSymbol symbol, boolean virtual) {
        String newAlias = null;
        if (virtual) {
            newAlias = "v_" + viewIndex++; //$NON-NLS-1$
        } else {
            newAlias = "g_" + groupIndex++; //$NON-NLS-1$
        }
        visitor.namingContext.groupNames.put(symbol.getName().toUpperCase(), newAlias);
        return newAlias;
    }
    
    public void visit(ScalarSubquery obj) {
        visitor.createChildNamingContext(false);
        visitNode(obj.getCommand());
        visitor.removeChildNamingContext();
    }
    
    public void visit(SubqueryCompareCriteria obj) {
        visitNode(obj.getLeftExpression());
        visitor.createChildNamingContext(false);
        visitNode(obj.getCommand());
        visitor.removeChildNamingContext();
    }
    
    public void visit(SubquerySetCriteria obj) {
        visitNode(obj.getExpression());
        visitor.createChildNamingContext(false);
        visitNode(obj.getCommand());
        visitor.removeChildNamingContext();
    }
    
    public void visit(ExistsCriteria obj) {
        visitor.createChildNamingContext(false);
        visitNode(obj.getCommand());
        visitor.removeChildNamingContext();
    }
    
    public void visit(OrderBy obj) {
        for (int i = 0; i < obj.getVariableCount(); i++) {
            SingleElementSymbol element = obj.getVariable(i);
            String name = visitor.namingContext.getElementName(null, element.getShortCanonicalName());
            boolean needsAlias = true;
            
            Expression expr = SymbolMap.getExpression(element);
                        
            if (!(expr instanceof SingleElementSymbol)) {
                expr = new ExpressionSymbol(element.getShortName(), expr);
            } else if (expr instanceof ElementSymbol) {
                needsAlias = needsAlias(name, (ElementSymbol)expr);
            } 
                        
            if (needsAlias) {
                element = new AliasSymbol(element.getShortName(), (SingleElementSymbol)expr);
                obj.getVariables().set(i, element);
            }
            element.setOutputName(name);
        }
    }
}