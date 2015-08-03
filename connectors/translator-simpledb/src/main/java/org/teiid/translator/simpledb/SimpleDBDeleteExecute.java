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

package org.teiid.translator.simpledb;

import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public class SimpleDBDeleteExecute implements UpdateExecution {

    private SimpleDBConnection connection;
    private int updatedCount=0;
    private SimpleDBDeleteVisitor visitor;

    public SimpleDBDeleteExecute(Command command, SimpleDBConnection connection) throws TranslatorException {
        this.connection = connection;
        this.visitor = new SimpleDBDeleteVisitor((Delete)command);
        this.visitor.checkExceptions();
    }

    public void execute() throws TranslatorException {
        String domainName = SimpleDBMetadataProcessor.getName(this.visitor.getTable());
        if (this.visitor.getCriteria() != null) {
            this.updatedCount = this.connection.performDelete(domainName, buildSelect());
        }
        else {
            // this is domain delete. otherwise this could be lot of items. deleted count can 
            // not be measured.
            this.connection.deleteDomain(domainName);
        }
    }    
    
    private String buildSelect() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(SimpleDBMetadataProcessor.ITEM_NAME); //$NON-NLS-1$
        sb.append(" FROM ").append(SimpleDBMetadataProcessor.getName(this.visitor.getTable())); //$NON-NLS-1$
        sb.append(" WHERE ").append(this.visitor.getCriteria()); //$NON-NLS-1$
        return sb.toString();
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        return new int[] { updatedCount };
    }
    
    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }    
}
