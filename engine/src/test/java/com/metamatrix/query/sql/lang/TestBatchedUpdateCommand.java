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

package com.metamatrix.query.sql.lang;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.query.metadata.BasicQueryMetadata;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;


/** 
 * @since 4.3
 */
public class TestBatchedUpdateCommand extends TestCase{
    public TestBatchedUpdateCommand(String name) { 
        super(name);
    }   
     
    private Delete getDeleteCommand() {
        Delete delete = new Delete();
        GroupSymbol group = new GroupSymbol("m.g");//$NON-NLS-1$
        group.setMetadataID("g1"); //$NON-NLS-1$
        delete.setGroup(group); 
        Option option = new Option();
        option.setShowPlan(true);       
        delete.setOption(option);
        return delete;
    }
    
    private Insert getInsertCommand() {
        Insert insert = new Insert();
        GroupSymbol group = new GroupSymbol("m.g");//$NON-NLS-1$
        group.setMetadataID("g2"); //$NON-NLS-1$
        insert.setGroup(group); 
        List vars = new ArrayList();
        vars.add(new ElementSymbol("a"));         //$NON-NLS-1$
        insert.setVariables(vars);
        List values = new ArrayList();
        values.add(new Reference(0));
        insert.setValues(values);
        Option option = new Option();
        option.setShowPlan(true);       
        insert.setOption(option);
        return insert;
    }    
    
    private Update getUpdateCommand() {
        Update update = new Update();
        GroupSymbol group = new GroupSymbol("m.g");//$NON-NLS-1$
        group.setMetadataID("g3"); //$NON-NLS-1$
        update.setGroup(group); 
        return update;
    }
    
    private Query getSelectCommand() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        g.setMetadataID("2");//$NON-NLS-1$
        From from = new From();
        from.addGroup(g);

        AliasSymbol as = new AliasSymbol("myA", new ElementSymbol("a")); //$NON-NLS-1$ //$NON-NLS-2$
        Select select = new Select();
        select.addSymbol(as);
        select.addSymbol(new ElementSymbol("b")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        
        return query;
    }
    
    private Query getSelectIntoCommand() {
        GroupSymbol g = new GroupSymbol("g"); //$NON-NLS-1$
        From from = new From();
        from.addGroup(g);
        Into into = new Into();
        into.setGroup(new GroupSymbol("intoGroup"));//$NON-NLS-1$
        
        AliasSymbol as = new AliasSymbol("myA", new ElementSymbol("a")); //$NON-NLS-1$ //$NON-NLS-2$
        Select select = new Select();
        select.addSymbol(as);
        select.addSymbol(new ElementSymbol("b")); //$NON-NLS-1$

        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        query.setInto(into);
        
        return query;
    }
    
    class FakeMetadataInterface extends BasicQueryMetadata{
        public Object getModelID(Object groupID) {
            if("g1".equals(groupID)) {//$NON-NLS-1$
                return "1";//$NON-NLS-1$
            }
            if("g2".equals(groupID)) {//$NON-NLS-1$
                return "1";//$NON-NLS-1$
            }
            if("g3".equals(groupID)) {//$NON-NLS-1$
                return "2";//$NON-NLS-1$
            }
            return null;
        }
    }
    
    public void testBatchedUpdateCount1() throws Exception{
        ArrayList commands = new ArrayList();
        commands.add(getDeleteCommand());
        commands.add(getInsertCommand());
        BatchedUpdateCommand command = new BatchedUpdateCommand(commands);
        assertEquals(command.updatingModelCount(new FakeMetadataInterface()), 1);
    }
    
    public void testBatchedUpdateCount2() throws Exception {
        ArrayList commands = new ArrayList();
        commands.add(getDeleteCommand());
        commands.add(getSelectCommand());
        BatchedUpdateCommand command = new BatchedUpdateCommand(commands);
        assertEquals(command.updatingModelCount(new FakeMetadataInterface()), 1);
    }    
    
    public void testBatchedUpdateCount3() throws Exception {
        ArrayList commands = new ArrayList();
        commands.add(getInsertCommand());
        commands.add(getSelectCommand());
        BatchedUpdateCommand command = new BatchedUpdateCommand(commands);
        assertEquals(command.updatingModelCount(new FakeMetadataInterface()), 1);
    }

    public void testBatchedUpdateCount4() throws Exception {
        ArrayList commands = new ArrayList();
        commands.add(getSelectIntoCommand());
        BatchedUpdateCommand command = new BatchedUpdateCommand(commands);
        assertEquals(command.updatingModelCount(new FakeMetadataInterface()), 2);
    }
    
    public void testBatchedUpdateCount5() throws Exception{
        ArrayList commands = new ArrayList();
        commands.add(getDeleteCommand());
        commands.add(getUpdateCommand());
        BatchedUpdateCommand command = new BatchedUpdateCommand(commands);
        assertEquals(command.updatingModelCount(new FakeMetadataInterface()), 2);
    }
}
