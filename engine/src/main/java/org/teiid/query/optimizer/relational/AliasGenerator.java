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

package org.teiid.query.optimizer.relational;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.ElementSymbol.DisplayMode;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.util.SymbolMap;


/**
 * Adds safe (generated) aliases to the source command
 * 
 * The structure is a little convoluted:
 * AliasGenerator - structure navigator, alters the command by adding alias symbols
 * NamingVisitor - changes the output names of Element and Group symbols
 * SQLNamingContext - a hierarchical context for tracking Element and Group names
 */
public class AliasGenerator extends PreOrderNavigator {
    
    private static final String table_prefix = "g_"; //$NON-NLS-1$
	private static final String view_prefix = "v_"; //$NON-NLS-1$

	private static class NamingVisitor extends LanguageVisitor {

        private class SQLNamingContext {
            SQLNamingContext parent;
            
            Map<String, Map<String, String>> elementMap = new HashMap<String, Map<String, String>>();
            Map<String, String> groupNames = new HashMap<String, String>();
            LinkedHashMap<Expression, String> currentSymbols;
            
            boolean aliasColumns = false;
            
            public SQLNamingContext(SQLNamingContext parent) {
                this.parent = parent;
            }
            
            public String getElementName(Expression symbol) {
            	if (!(symbol instanceof ElementSymbol)) {
            		return null;
            	}
            	ElementSymbol element = (ElementSymbol)symbol;
            	String newGroupName = this.groupNames.get(element.getGroupSymbol().getName());
            	if (newGroupName == null) {
            		if (parent == null) {
            			return null;
            		}
                	return parent.getElementName(symbol);
            	}
            	//check for inline view
            	Map<String, String> elements = this.elementMap.get(element.getGroupSymbol().getName());
            	if (elements != null) {
            		String name = elements.get(element.getShortName());
            		if (name != null) {
            			renameGroup(element.getGroupSymbol(), newGroupName);
            			return name;
            		}
                }
            	if (parent != null) {
            		String name = parent.getElementName(symbol);
            		if (name != null) {
            			return name;
            		}
            	}
            	renameGroup(element.getGroupSymbol(), newGroupName);
            	return null;
            }
            
            public void renameGroup(GroupSymbol obj, String newAlias) {
                if (aliasGroups) {
                    String definition = obj.getNonCorrelationName();
                    if (newAlias == null) {
                        return;
                    }
                    obj.setName(newAlias);
                    obj.setDefinition(definition);
                } else if(obj.getDefinition() != null) {
                    obj.setName(obj.getDefinition());
                    obj.setDefinition(null);
                } else {
                	obj.setOutputName(null);
                	obj.setOutputDefinition(null);
                }
            }
                    
            private String getGroupName(String group) {
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
    	
        private SQLNamingContext namingContext = new SQLNamingContext(null);
        boolean aliasGroups;
        
        public NamingVisitor(boolean aliasGroups) {
            this.aliasGroups = aliasGroups;
        }
                
        /** 
         * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.symbol.ElementSymbol)
         */
        @Override
        public void visit(ElementSymbol obj) {
            GroupSymbol group = obj.getGroupSymbol();
            if(group == null) {
                return;
            }
            String newName = namingContext.getElementName(obj);
            
            if (newName != null) {
                obj.setShortName(newName);
            }
            obj.setDisplayMode(ElementSymbol.DisplayMode.FULLY_QUALIFIED);
        }
        
        /** 
         * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.symbol.GroupSymbol)
         */
        @Override
        public void visit(GroupSymbol obj) {
        	this.namingContext.renameGroup(obj, this.namingContext.getGroupName(obj.getName()));
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
    private int groupIndex;
    private int viewIndex;
    private boolean stripColumnAliases;
	private Map<String, String> aliasMapping;

    public AliasGenerator(boolean aliasGroups) {
    	this(aliasGroups, false);
    }
    
    public AliasGenerator(boolean aliasGroups, boolean stripColumnAliases) {
        super(new NamingVisitor(aliasGroups));
        this.visitor = (NamingVisitor)this.getVisitor();
        this.stripColumnAliases = stripColumnAliases;
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
    	List<Expression> selectSymbols = obj.getSymbols();
        LinkedHashMap<Expression, String> symbols = new LinkedHashMap<Expression, String>(selectSymbols.size());                
        for (int i = 0; i < selectSymbols.size(); i++) {
            Expression symbol = selectSymbols.get(i);
            visitNode(symbol);
            boolean needsAlias = visitor.namingContext.aliasColumns;
            
            String newAlias = "c_" + i; //$NON-NLS-1$
            
            Expression newSymbol = SymbolMap.getExpression(symbol);
            
            if (newSymbol instanceof ElementSymbol) {
            	if (!needsAlias) {
            		newAlias = ((ElementSymbol)newSymbol).getShortName();
            	} else {
                    needsAlias &= needsAlias(newAlias, (ElementSymbol)newSymbol);
                }
            }
                        
            symbols.put(symbol, newAlias);
            if (visitor.namingContext.aliasColumns && needsAlias) {
                newSymbol = new AliasSymbol(Symbol.getShortName(symbol), newSymbol);
                ((AliasSymbol)newSymbol).setShortName(newAlias);
            } 
            selectSymbols.set(i, newSymbol);
        }
        
        visitor.namingContext.currentSymbols = symbols; 
    }

    private boolean needsAlias(String newAlias,
                               ElementSymbol symbol) {
        return !(symbol.getMetadataID() instanceof TempMetadataID) || !newAlias.equalsIgnoreCase(symbol.getShortName());
    }
    
    /**
     * visit the query in definition order
     */
    public void visit(Query obj) {
        if (obj.getOrderBy() != null || obj.getLimit() != null) {
            visitor.namingContext.aliasColumns = !stripColumnAliases;
        }        
        visitNode(obj.getFrom());
        if (this.aliasMapping != null) {
        	HashSet<String> newSymbols = new HashSet<String>();
        	for (Map.Entry<String, String> entry : this.visitor.namingContext.groupNames.entrySet()) {
        		if (!newSymbols.add(entry.getValue())) {
        			throw new TeiidRuntimeException(new QueryPlannerException(QueryPlugin.Event.TEIID31126, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31126, entry.getValue())));
        		}
        	}
        }
        visitNode(obj.getCriteria());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getSelect());
        visitNode(obj.getOrderBy());
    }
    
    public void visit(SubqueryFromClause obj) {
        visitor.createChildNamingContext(true);
        //first determine the original names
        List<Expression> exprs = obj.getCommand().getProjectedSymbols();
        List<String> names = new ArrayList<String>(exprs.size());
        for (int i = 0; i < exprs.size(); i++) {
        	names.add(Symbol.getShortName(exprs.get(i)));
        }
        obj.getCommand().acceptVisitor(this);
        Map<String, String> viewGroup = new HashMap<String, String>();
        int i = 0;
        //now map to the new names
        for (Entry<Expression, String> entry : visitor.namingContext.currentSymbols.entrySet()) {
        	viewGroup.put(names.get(i++), entry.getValue());
        }
        visitor.namingContext.parent.elementMap.put(obj.getName(), viewGroup);
        visitor.removeChildNamingContext();
        obj.getGroupSymbol().setName(recontextGroup(obj.getGroupSymbol(), true));
    }
    
    @Override
    public void visit(UnaryFromClause obj) {
        GroupSymbol symbol = obj.getGroup();
        if (visitor.aliasGroups) {
            recontextGroup(symbol, false);
        } else {
        	visitor.namingContext.groupNames.put(symbol.getName(), symbol.getNonCorrelationName());
        }
        super.visit(obj);
    }

    /** 
     * @param symbol
     */
    private String recontextGroup(GroupSymbol symbol, boolean virtual) {
        String newAlias = null;
        if (virtual) {
            newAlias = view_prefix + viewIndex++; 
        } else {
            newAlias = table_prefix + groupIndex++; 
        }
        if (this.aliasMapping != null && symbol.getDefinition() != null) {
        	String oldAlias = this.aliasMapping.get(symbol.getName());
        	if (oldAlias != null) {
        		newAlias = oldAlias;
            	if (newAlias.startsWith(table_prefix) || newAlias.startsWith(view_prefix)) {
            		try {
            			Integer.parseInt(newAlias.substring(2, newAlias.length()));
            			throw new TeiidRuntimeException(new QueryPlannerException(QueryPlugin.Event.TEIID31127, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31127, newAlias)));
            		} catch (NumberFormatException e) {
            			
            		}
            	}
        	}
        }
        visitor.namingContext.groupNames.put(symbol.getName(), newAlias);
        return newAlias;
    }
    
    public void visit(ScalarSubquery obj) {
    	if (obj.shouldEvaluate()) {
    		return;
    	}
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
    	if (obj.shouldEvaluate()) {
    		return;
    	}
        visitor.createChildNamingContext(false);
        visitNode(obj.getCommand());
        visitor.removeChildNamingContext();
    }
    
    public void visit(OrderBy obj) {
    	//add/correct aliases if necessary
        for (int i = 0; i < obj.getVariableCount(); i++) {
            OrderByItem item = obj.getOrderByItems().get(i);
            Expression element = item.getSymbol();
            visitNode(element);
            
            Expression expr = SymbolMap.getExpression(element);
            
            if (item.isUnrelated()) {
                item.setSymbol(expr);
            	continue;
            }
            String name = null;
            if (visitor.namingContext.currentSymbols != null) {
        		name = visitor.namingContext.currentSymbols.get(element);
        	}
            if (name == null) {
            	//this is a bit messy, because we have cloned to do the aliasing, there
            	//is a chance that a subquery is throwing off the above get
            	int pos = item.getExpressionPosition();
            	if (pos < visitor.namingContext.currentSymbols.size()) {
            		ArrayList<Map.Entry<Expression, String>> list = new ArrayList<Map.Entry<Expression,String>>(visitor.namingContext.currentSymbols.entrySet());
            		name = list.get(pos).getValue();
            		expr = SymbolMap.getExpression(list.get(pos).getKey());
            	} else {
            		name = Symbol.getShortName(element);
            	}
            }
            boolean needsAlias = visitor.namingContext.aliasColumns;
            if (name == null) {
	            continue;
            }
                        
            if (expr instanceof ElementSymbol) {
            	needsAlias &= needsAlias(name, (ElementSymbol)expr);
            } 
                        
            if (needsAlias) {
                element = new AliasSymbol(Symbol.getShortName(element), expr);
            } else {
            	element = expr;
            	if (expr instanceof ElementSymbol && visitor.namingContext.aliasColumns) {
            		((ElementSymbol)expr).setDisplayMode(DisplayMode.SHORT_OUTPUT_NAME);
            	}
            }
            item.setSymbol(element);
            if (element instanceof Symbol) {
            	((Symbol)element).setShortName(name);
            }
        }
    }
    
    public void visit(Reference obj) {
    	//we need to follow references to correct correlated variables
        visitNode(obj.getExpression());
    }

	public void setAliasMapping(Map<String, String> aliasMapping) {
		this.aliasMapping = aliasMapping;
	}

}