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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
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
    
    private static class NamingVisitor extends LanguageVisitor {

        private class SQLNamingContext {
            SQLNamingContext parent;
            
            Map<String, Map<String, String>> elementMap = new HashMap<String, Map<String, String>>();
            Map<String, String> groupNames = new HashMap<String, String>();
            Map<Expression, String> currentSymbols;
            
            boolean aliasColumns = false;
            
            public SQLNamingContext(SQLNamingContext parent) {
                this.parent = parent;
            }
            
            public String getElementName(Expression symbol, boolean renameGroup) {
            	String name = null;
            	if (currentSymbols != null) {
            		name = currentSymbols.get(symbol);
                	if (name != null) {
                		if (renameGroup && symbol instanceof ElementSymbol) {
            				renameGroup(((ElementSymbol)symbol).getGroupSymbol());
            			}
                		return name;
                	}
            	}
            	if (!(symbol instanceof ElementSymbol)) {
            		return null;
            	}
            	ElementSymbol element = (ElementSymbol)symbol;
            	Map<String, String> elements = this.elementMap.get(element.getGroupSymbol().getName());
            	if (elements != null) {
            		name = elements.get(element.getShortName());
            		if (name != null) {
            			if (renameGroup) {
            				renameGroup(element.getGroupSymbol());
            			}
            			return name;
            		}
                }
                if (parent != null) {
                	name = parent.getElementName(symbol, renameGroup);
                	if (name != null) {
                		return name;
                	}
                }
            	if (renameGroup) {
    				renameGroup(element.getGroupSymbol());
    			}
            	return null;
            }
            
            public void renameGroup(GroupSymbol obj) {
                if (aliasGroups) {
                    String definition = obj.getNonCorrelationName();
                    String newAlias = getGroupName(obj.getName());
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
            String newName = namingContext.getElementName(obj, true);
            
            if (newName == null) {
                newName = Symbol.getShortName(obj.getOutputName());
            }
            
            obj.setOutputName(group.getOutputName() + Symbol.SEPARATOR + newName);
            obj.setDisplayMode(ElementSymbol.DisplayMode.OUTPUT_NAME);
        }
        
        /** 
         * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.symbol.GroupSymbol)
         */
        @Override
        public void visit(GroupSymbol obj) {
        	this.namingContext.renameGroup(obj);
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
        super.visit(obj);
        List<Expression> selectSymbols = obj.getSymbols();
        HashMap<Expression, String> symbols = new HashMap<Expression, String>(selectSymbols.size());                
        for (int i = 0; i < selectSymbols.size(); i++) {
            Expression symbol = selectSymbols.get(i);

            boolean needsAlias = visitor.namingContext.aliasColumns;
            
            String newAlias = "c_" + i; //$NON-NLS-1$
            
            Expression newSymbol = SymbolMap.getExpression(symbol);
            
            if (newSymbol instanceof ElementSymbol) {
            	if (!needsAlias) {
            		newAlias = ((ElementSymbol)newSymbol).getOutputName();
            	} else {
                    needsAlias &= needsAlias(newAlias, (ElementSymbol)newSymbol);
                }
            }
                        
            symbols.put(symbol, newAlias);
            if (visitor.namingContext.aliasColumns && needsAlias) {
                newSymbol = new AliasSymbol(Symbol.getShortName(symbol), newSymbol);
                ((AliasSymbol)newSymbol).setOutputName(newAlias);
            } 
            selectSymbols.set(i, newSymbol);
        }
        
        visitor.namingContext.currentSymbols = symbols; 
    }

    private boolean needsAlias(String newAlias,
                               ElementSymbol symbol) {
        return !(symbol.getMetadataID() instanceof TempMetadataID) || !newAlias.equalsIgnoreCase(visitor.namingContext.getElementName(symbol, false));
    }
    
    /**
     * visit the query in definition order
     */
    public void visit(Query obj) {
        if (obj.getOrderBy() != null || obj.getLimit() != null) {
            visitor.namingContext.aliasColumns = !stripColumnAliases;
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
        Map<String, String> viewGroup = new HashMap<String, String>();
        for (Entry<Expression, String> entry : visitor.namingContext.currentSymbols.entrySet()) {
        	viewGroup.put(Symbol.getShortName(entry.getKey()), entry.getValue());
        }
        visitor.namingContext.parent.elementMap.put(obj.getName(), viewGroup);
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
            if (item.isUnrelated()) {
            	visitNode(element);
            	continue;
            }
            String name = visitor.namingContext.getElementName(element, false);
            boolean needsAlias = visitor.namingContext.aliasColumns;
            if (name == null) {
	            continue;
            }
            Expression expr = SymbolMap.getExpression(element);
                        
            if (expr instanceof ElementSymbol) {
            	needsAlias &= needsAlias(name, (ElementSymbol)expr);
            } 
                        
            if (needsAlias) {
                element = new AliasSymbol(Symbol.getShortName(element), expr);
            } else if (expr instanceof ElementSymbol) {
            	element = expr;
            	visitNode(element);
            }
            item.setSymbol(element);
            if (element instanceof Symbol) {
            	((Symbol)element).setOutputName(name);
            }
        }
    }
    
    public void visit(Reference obj) {
    	//we need to follow references to correct correlated variables
        visitNode(obj.getExpression());
    }

}