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
package org.teiid.translator.simpledb.visitors;

import static org.junit.Assert.assertEquals;

import java.util.*;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.*;
import org.teiid.language.Argument.Direction;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.simpledb.SimpleDBExecutionFactory;
import org.teiid.translator.simpledb.SimpleDBInsertVisitor;

import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectResult;

@SuppressWarnings("nls")
public class TestSimpleDBExecution {
    private static SimpleDBExecutionFactory translator;
    private static TranslationUtility utility;
    private static SimpleDBConnection connection;

    @Before
    public void setup() throws Exception {        
        translator = new SimpleDBExecutionFactory();
        translator.start();
        
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table item (\"itemName()\" string, attribute string, somedate timestamp, strarray string[]);", "x", "y");
        utility = new TranslationUtility(tm);
        
        connection = Mockito.mock(SimpleDBConnection.class);
    }

    @Test
    public void testSelect() throws Exception {
        SelectResult result = new SelectResult();
        result.setItems(mockResult());
        String query = "select * from item where attribute > 'name'";        
        Mockito.stub(connection.performSelect(Mockito.anyString(), Mockito.anyString())).toReturn(result);
        
        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        ResultSetExecution exec = translator.createResultSetExecution((Select)cmd, context, Mockito.mock(RuntimeMetadata.class), connection);
        exec.execute();
        exec.next();
        
        Mockito.verify(connection).performSelect("SELECT attribute, somedate, strarray FROM item WHERE attribute > 'name'", null);
    }
    
    @Test
    public void testUpdate() throws Exception {
        String query = "UPDATE item set attribute = 'value', somedate = {ts '2014-04-04 10:50:45'} where attribute > 'name'";
        Mockito.stub(connection.performUpdate(Mockito.anyString(),Mockito.anyMap(), Mockito.anyString())).toReturn(100);
        
        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        UpdateExecution exec = translator.createUpdateExecution(cmd, context, Mockito.mock(RuntimeMetadata.class), connection);
        exec.execute();
        
        Map<String, Object> attributes = new TreeMap<String, Object>();
        attributes.put("attribute", "value");
        attributes.put("somedate", "2014-04-04 10:50:45.0");
        Mockito.verify(connection).performUpdate("item", attributes, "SELECT itemName() FROM item WHERE attribute > 'name'");
    }  
    
    @Test
    public void testUpdateArray() throws Exception {
        String query = "UPDATE item set strarray = ('1','2')";
        Mockito.stub(connection.performUpdate(Mockito.anyString(),Mockito.anyMap(), Mockito.anyString())).toReturn(100);
        
        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        UpdateExecution exec = translator.createUpdateExecution(cmd, context, Mockito.mock(RuntimeMetadata.class), connection);
        exec.execute();
        
        ArgumentCaptor<String> item = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> select = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<HashMap> args = ArgumentCaptor.forClass(HashMap.class);
        Mockito.verify(connection, Mockito.times(1)).performUpdate(item.capture(), args.capture(), select.capture());
        System.out.println();
        
        assertEquals("1,2,", arrayToStr((String[])args.getAllValues().get(0).get("strarray")));
        assertEquals("item", item.getAllValues().get(0));
        assertEquals("SELECT itemName() FROM item", select.getAllValues().get(0));
    }     
    
    @Test
    public void testDelete() throws Exception {
        String query = "delete from item where somedate = {ts '2014-04-04 10:50:45'}";
        Mockito.stub(connection.performDelete(Mockito.anyString(), Mockito.anyString())).toReturn(100);
        
        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        UpdateExecution exec = translator.createUpdateExecution(cmd, context, Mockito.mock(RuntimeMetadata.class), connection);
        exec.execute();
        
        Mockito.verify(connection).performDelete("item", "SELECT itemName() FROM item WHERE somedate = '2014-04-04 10:50:45.0'");
    }     
    
    @Test
    public void testDeleteAll() throws Exception {
        String query = "delete from item";
        
        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        UpdateExecution exec = translator.createUpdateExecution(cmd, context, Mockito.mock(RuntimeMetadata.class), connection);
        exec.execute();
        
        Mockito.verify(connection).deleteDomain("item");
    }     
    
    @Test
    public void testInsert() throws Exception {
        String query = "insert into item (\"itemName()\", attribute, somedate, strarray) values ('one', 'value', {ts '2014-04-04 10:50:45'}, ('1', '2'))";
        
        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        UpdateExecution exec = translator.createUpdateExecution(cmd, context, Mockito.mock(RuntimeMetadata.class), connection);
        exec.execute();
        
        SimpleDBInsertVisitor visitor = new SimpleDBInsertVisitor();
        visitor.visitNode(cmd);
        
        ArgumentCaptor<Iterator> argument = ArgumentCaptor.forClass(Iterator.class);
        ArgumentCaptor<String> itemName = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ArrayList> columns = ArgumentCaptor.forClass(ArrayList.class);
        
        Mockito.verify(connection).performInsert(itemName.capture(), columns.capture(), argument.capture());
        
        assertEquals("item", itemName.getValue());
        List<?> values = (List<?>)argument.getValue().next();
        assertEquals("value", values.get(1));
        assertEquals("2014-04-04 10:50:45.0", values.get(2).toString());
        assertEquals("1,2,", arrayToStr((String[])values.get(3)));
    }     
    
    private String arrayToStr(String[] array) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < array.length; i++) {
            sb.append(array[i]).append(",");
        }
        return sb.toString();
    }

    private List<Item> mockResult() {
        List<Attribute> attributes = new ArrayList<Attribute>();
        attributes.add(new Attribute("a1", "a1"));
        attributes.add(new Attribute("a2", "a2"));
        attributes.add(new Attribute("a2", "a22"));
        
        List<Item> items = new ArrayList<Item>();
        items.add(new Item("one", attributes));
        return items;
    }

    @Test
    public void testDirectExecution() throws Exception {
        SelectResult result = new SelectResult();
        result.setItems(mockResult());
        
        String query = "select * from item where attribute > 'name'";
        
        Mockito.stub(connection.performSelect(Mockito.anyString(), Mockito.anyString())).toReturn(result);
        
        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        
        List<Argument> arguments = new ArrayList<Argument>();
        Argument arg = new Argument(Direction.IN, String.class, Mockito.mock(ProcedureParameter.class));
        arg.setArgumentValue(LanguageFactory.INSTANCE.createLiteral(query, String.class));
        arguments.add(arg);
        
        
        ResultSetExecution exec = translator.createDirectExecution(arguments, cmd, context, Mockito.mock(RuntimeMetadata.class), connection);
        exec.execute();
        List row = exec.next();
        
        Mockito.verify(connection).performSelect("select * from item where attribute > 'name'", null);
        
        Object[] results = (Object[])row.get(0);
        assertEquals("a1", results[0]);
        assertEquals("[a2, a22]", results[1]);
    }
}
