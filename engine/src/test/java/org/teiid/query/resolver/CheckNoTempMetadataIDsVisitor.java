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

package org.teiid.query.resolver;

import java.util.ArrayList;
import java.util.Collection;

import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;


/**
 * Used to verify that all symbols in a LanguageObject were resolved
 * with respect to runtime metadata - also that they aren't resolved
 * to TemporaryMetadatID's.
 */
public class CheckNoTempMetadataIDsVisitor extends LanguageVisitor {

    private Collection<Symbol> symbolsWithTempMetadataIDs;
    
    /**
     * By default, this visitor deeply traverses all commands, and there are
     * no symbols to ignore
     */
	public CheckNoTempMetadataIDsVisitor() { 
        symbolsWithTempMetadataIDs = new ArrayList<Symbol>();    
    }
    
    /**
     * Get the Collection of any unresolved symbols
     * @return Collection of any unresolved Symbols; may
     * be empty but never null
     */
    public Collection<Symbol> getSymbols(){
        return this.symbolsWithTempMetadataIDs;
    }

    /**
     * By default, this visitor deeply traverses all commands, and there are
     * no symbols to ignore
     */
    public static final Collection<Symbol> checkSymbols(LanguageObject obj){
        CheckNoTempMetadataIDsVisitor visitor = new CheckNoTempMetadataIDsVisitor();
        DeepPreOrderNavigator.doVisit(obj, visitor);
        return visitor.getSymbols();
    }

    // visitor methods
    
    public void visit(ElementSymbol obj) {
        if (obj.getMetadataID() instanceof TempMetadataID){
            this.symbolsWithTempMetadataIDs.add(obj);
        }
    }

    public void visit(GroupSymbol obj) {
        if (obj.getMetadataID() instanceof TempMetadataID){
            this.symbolsWithTempMetadataIDs.add(obj);
        }
    }

}
