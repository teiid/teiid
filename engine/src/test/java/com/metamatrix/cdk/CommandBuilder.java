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

package com.metamatrix.cdk;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.language.LanguageFactory;
import org.teiid.dqp.internal.datamgr.language.LanguageBridgeFactory;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.relational.AliasGenerator;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.symbol.AllSymbol;

/**
 * Convert a query string into a SQL language parse tree.
 */
public class CommandBuilder {
	
	public static LanguageFactory getLanuageFactory() {
		return LanguageFactory.INSTANCE;
	}

    private QueryMetadataInterface metadata;
    
    /**
     * @param metadata The metadata describing the datasource which the query is for.
     */
    public CommandBuilder(QueryMetadataInterface metadata) {
        this.metadata = metadata;
    }
    
    public org.teiid.connector.language.Command getCommand(String queryString) {
        return getCommand(queryString, false, false);
    }
    
    public org.teiid.connector.language.Command getCommand(String queryString, boolean generateAliases, boolean supportsGroupAlias) {
        Command command = null;
        try {
            command = QueryParser.getQueryParser().parseCommand(queryString);
            QueryResolver.resolveCommand(command, metadata);
            command = QueryRewriter.rewrite(command, metadata, null);
            expandAllSymbol(command);            
            if (generateAliases) {
                command.acceptVisitor(new AliasGenerator(supportsGroupAlias));
            }
            return new LanguageBridgeFactory(metadata).translate(command);
        } catch (MetaMatrixException e) {
            throw new MetaMatrixRuntimeException(e);
		}
    }
    
    /**
     * Convert the "*" in "select * from..." to the list of column names for the data source.
     */
    protected void expandAllSymbol(Command command) {
        if (command instanceof Query) {
            Query query = (Query) command;
            Select select = query.getSelect();
            List originalSymbols = select.getSymbols();
            List expandedSymbols = new ArrayList();
            for (Iterator i = originalSymbols.iterator(); i.hasNext(); ) {
                Object next = i.next();
                if (next instanceof AllSymbol) {
                    AllSymbol allSymbol = (AllSymbol) next;
                    expandedSymbols.addAll(allSymbol.getElementSymbols());
                } else {
                    expandedSymbols.add(next);
                }
            }
            select.setSymbols(expandedSymbols);
        }
    }
    
}
