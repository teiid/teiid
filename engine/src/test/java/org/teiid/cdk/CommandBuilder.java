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

package org.teiid.cdk;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.dqp.internal.datamgr.LanguageBridgeFactory;
import org.teiid.language.LanguageFactory;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.relational.AliasGenerator;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.navigator.DeepPostOrderNavigator;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;


/**
 * Convert a query string into a SQL language parse tree.
 */
public class CommandBuilder {
	
	public static LanguageFactory getLanuageFactory() {
		return LanguageFactory.INSTANCE;
	}

    private QueryMetadataInterface metadata;
	private LanguageBridgeFactory languageBridgeFactory;
    
    /**
     * @param metadata The metadata describing the datasource which the query is for.
     */
    public CommandBuilder(QueryMetadataInterface metadata) {
        this.metadata = metadata;
        this.languageBridgeFactory = new LanguageBridgeFactory(metadata);
    }
    
    public LanguageBridgeFactory getLanguageBridgeFactory() {
		return languageBridgeFactory;
	}
    
    public org.teiid.language.Command getCommand(String queryString) {
        return getCommand(queryString, false, false);
    }
    
    public org.teiid.language.Command getCommand(String queryString, boolean generateAliases, boolean supportsGroupAlias) {
        Command command = null;
        try {
            command = QueryParser.getQueryParser().parseCommand(queryString);
            QueryResolver.resolveCommand(command, metadata);
            command = QueryRewriter.rewrite(command, metadata, null);
            expandAllSymbol(command);            
            if (generateAliases) {
            	command = (Command)command.clone();
                command.acceptVisitor(new AliasGenerator(supportsGroupAlias));
            }
            //the language bridge doesn't expect References
            for (SubqueryContainer<?> container : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(command)) {
        			ExpressionMappingVisitor visitor = new ExpressionMappingVisitor(null) {
        				@Override
        				public Expression replaceExpression(Expression element) {
        					if (element instanceof Reference) {
        						return ((Reference)element).getExpression();
        					}
        					return element;
        				}
        			};
        			DeepPostOrderNavigator.doVisit(command, visitor);
    		}
			return languageBridgeFactory.translate(command);
        } catch (TeiidException e) {
            throw new TeiidRuntimeException(e);
		}
    }
    
    /**
     * Convert the "*" in "select * from..." to the list of column names for the data source.
     */
    protected void expandAllSymbol(Command command) {
        if (command instanceof Query) {
            Query query = (Query) command;
            Select select = query.getSelect();
            List<Expression> originalSymbols = select.getSymbols();
            List<Expression> expandedSymbols = new ArrayList<Expression>();
            for (Iterator<Expression> i = originalSymbols.iterator(); i.hasNext(); ) {
                Expression next = i.next();
                if (next instanceof MultipleElementSymbol) {
                    MultipleElementSymbol allSymbol = (MultipleElementSymbol) next;
                    expandedSymbols.addAll(allSymbol.getElementSymbols());
                } else {
                    expandedSymbols.add(next);
                }
            }
            select.setSymbols(expandedSymbols);
        }
    }
    
}
