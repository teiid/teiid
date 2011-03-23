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

package org.teiid.query.processor.xml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.mapping.xml.MappingNodeConstants;
import org.teiid.query.mapping.xml.ResultSetInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.xml.AddNodeInstruction;
import org.teiid.query.processor.xml.Condition;
import org.teiid.query.processor.xml.CriteriaCondition;
import org.teiid.query.processor.xml.EndDocumentInstruction;
import org.teiid.query.processor.xml.ExecSqlInstruction;
import org.teiid.query.processor.xml.IfInstruction;
import org.teiid.query.processor.xml.InitializeDocumentInstruction;
import org.teiid.query.processor.xml.MoveCursorInstruction;
import org.teiid.query.processor.xml.MoveDocInstruction;
import org.teiid.query.processor.xml.NodeDescriptor;
import org.teiid.query.processor.xml.ProcessorInstruction;
import org.teiid.query.processor.xml.Program;
import org.teiid.query.processor.xml.WhileInstruction;
import org.teiid.query.processor.xml.XMLPlan;
import org.teiid.query.processor.xml.XMLProcessorEnvironment;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.unittest.FakeMetadataFacade;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.unittest.FakeMetadataObject;
import org.teiid.query.unittest.FakeMetadataStore;
import org.teiid.query.util.CommandContext;

import junit.framework.TestCase;


/**
 */
public class TestInstructions extends TestCase {

    public TestInstructions(String name) {
        super(name);
    }

    /**
     * Construct some fake metadata.  Basic conceptual tree is:
     * 
     * stock (physical model)
     *   items (physical group)
     *     itemNum (string)
     *     itemName (string)
     *     itemQuantity (integer)
     * xmltest (virtual model)
     *   rs (virtual group / result set definition)
     *     itemNum (string)
     *     itemName (string)
     *     itemQuantity (integer)
     */
    public FakeMetadataFacade exampleMetadata() { 
        // Create models
        FakeMetadataObject stock = FakeMetadataFactory.createPhysicalModel("stock"); //$NON-NLS-1$
        FakeMetadataObject xmltest = FakeMetadataFactory.createVirtualModel("xmltest");     //$NON-NLS-1$

        // Create physical groups
        FakeMetadataObject items = FakeMetadataFactory.createPhysicalGroup("stock.items", stock); //$NON-NLS-1$
                
        // Create physical elements
        List itemElements = FakeMetadataFactory.createElements(items, 
            new String[] { "itemNum", "itemName", "itemQuantity" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });

        // Create virtual groups
        QueryNode rsQuery = new QueryNode("SELECT itemNum, itemName, itemQuantity FROM stock.items"); //$NON-NLS-1$ //$NON-NLS-2$
        FakeMetadataObject rs = FakeMetadataFactory.createVirtualGroup("xmltest.rs", xmltest, rsQuery); //$NON-NLS-1$

        // Create virtual elements
        List rsElements = FakeMetadataFactory.createElements(rs, 
            new String[] { "itemNum", "itemName", "itemQuantity" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });        
            
        // Add all objects to the store
        FakeMetadataStore store = new FakeMetadataStore();
        store.addObject(stock);
        store.addObject(items);
        store.addObjects(itemElements);

        store.addObject(xmltest);
        store.addObject(rs);
        store.addObjects(rsElements);
                        
        // Create the facade from the store
        return new FakeMetadataFacade(store);
    }
    
    private Command helpGetCommand(String sql, FakeMetadataFacade metadata) throws TeiidComponentException, TeiidProcessingException {
        // parse
        QueryParser parser = new QueryParser();
        Command command = parser.parseCommand(sql);
        
        QueryResolver.resolveCommand(command, metadata);
        command = QueryRewriter.rewrite(command, metadata, null);
        
        return command;        
    }

    private Criteria helpGetCriteria(String sql, FakeMetadataFacade metadata) throws QueryParserException, QueryResolverException, TeiidComponentException {
        QueryParser parser = new QueryParser();
        Criteria crit = parser.parseCriteria(sql);
   
        ResolverVisitor.resolveLanguageObject(crit, null, metadata);

        return crit;        
    }

    public void helpProcessInstructions(Program prog, XMLProcessorEnvironment env, String expected) throws Exception {
        env.pushProgram(prog);
    	XMLPlan plan = new XMLPlan(env);
    	TestProcessor.doProcess(plan, new FakeDataManager(), new List[] {Arrays.asList(expected)}, new CommandContext());
    }
    
    public Program exampleProgram(FakeMetadataFacade metadata, XMLProcessorEnvironment env) throws Exception{

        ProcessorInstruction i0 = new InitializeDocumentInstruction("UTF-8", true);         //$NON-NLS-1$
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor("Catalogs", null, AddNodeInstruction.ELEMENT, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i1 = new AddNodeInstruction(descriptor);
        ProcessorInstruction i2 = new MoveDocInstruction(MoveDocInstruction.DOWN);
        descriptor = NodeDescriptor.createNodeDescriptor("Catalog", null, AddNodeInstruction.ELEMENT, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i3 = new AddNodeInstruction(descriptor);
        ProcessorInstruction i4 = new MoveDocInstruction(MoveDocInstruction.DOWN);
        descriptor = NodeDescriptor.createNodeDescriptor("Items", null, AddNodeInstruction.ELEMENT, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i5 = new AddNodeInstruction(descriptor);
        ProcessorInstruction i6 = new MoveDocInstruction(MoveDocInstruction.DOWN);        
        
        ResultSetInfo info = new ResultSetInfo("xmltest.rs"); //$NON-NLS-1$
        ProcessorInstruction i7 = new ExecSqlInstruction("xmltest.rs", info);                 //$NON-NLS-1$
        ProcessorInstruction i8 = new MoveCursorInstruction("xmltest.rs"); //$NON-NLS-1$

        WhileInstruction i9 = new WhileInstruction("xmltest.rs");         //$NON-NLS-1$
        descriptor = NodeDescriptor.createNodeDescriptor("Item", null, AddNodeInstruction.ELEMENT, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i10 = new AddNodeInstruction(descriptor);
        ProcessorInstruction i11 = new MoveDocInstruction(MoveDocInstruction.DOWN);
        descriptor = NodeDescriptor.createNodeDescriptor("ItemID", null, AddNodeInstruction.ATTRIBUTE, null, null, null, false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i12 = new AddNodeInstruction(descriptor, new ElementSymbol("xmltest.rs.itemNum")); //$NON-NLS-1$
        descriptor = NodeDescriptor.createNodeDescriptor("Name", null, AddNodeInstruction.ELEMENT, null, null, null, false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i13 = new AddNodeInstruction(descriptor, new ElementSymbol("xmltest.rs.itemName"));//$NON-NLS-1$
        descriptor = NodeDescriptor.createNodeDescriptor("Quantity", null, AddNodeInstruction.ELEMENT, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i14 = new AddNodeInstruction(descriptor, new ElementSymbol("xmltest.rs.itemQuantity"));//$NON-NLS-1$
        ProcessorInstruction i15 = new MoveDocInstruction(MoveDocInstruction.UP);
        ProcessorInstruction i16 = new MoveCursorInstruction("xmltest.rs"); //$NON-NLS-1$

        ProcessorInstruction i17 = new MoveDocInstruction(MoveDocInstruction.UP);
        ProcessorInstruction i18 = new MoveDocInstruction(MoveDocInstruction.UP);
        ProcessorInstruction i19 = new MoveDocInstruction(MoveDocInstruction.UP);
        
        ProcessorInstruction i20 = new EndDocumentInstruction();
        
        // Stitch them together
        
        Program program = new Program();
        program.addInstruction(i0);
        program.addInstruction(i1);
        program.addInstruction(i2);
        program.addInstruction(i3);
        program.addInstruction(i4);
        program.addInstruction(i5);
        program.addInstruction(i6);
        program.addInstruction(i7);
        program.addInstruction(i8);
        program.addInstruction(i9);

        Program subProgram = new Program();
        i9.setBlockProgram(subProgram);
        subProgram.addInstruction(i10);
        subProgram.addInstruction(i11);
        subProgram.addInstruction(i12);
        subProgram.addInstruction(i13);
        subProgram.addInstruction(i14);
        subProgram.addInstruction(i15);
        subProgram.addInstruction(i16);

        program.addInstruction(i17);
        program.addInstruction(i18);
        program.addInstruction(i19);
        program.addInstruction(i20);

        //ProgramUtil.printProgram(program);
        return program;        
    }

    public Program exampleProgram2(Criteria crit, FakeMetadataFacade metadata, XMLProcessorEnvironment env) throws Exception{
        ProcessorInstruction i0 = new InitializeDocumentInstruction("UTF-8", true);         //$NON-NLS-1$
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor("Catalogs", null, AddNodeInstruction.ELEMENT, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i1 = new AddNodeInstruction(descriptor);
        ProcessorInstruction i2 = new MoveDocInstruction(MoveDocInstruction.DOWN);
        descriptor = NodeDescriptor.createNodeDescriptor("Catalog", null, AddNodeInstruction.ELEMENT, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i3 = new AddNodeInstruction(descriptor);
        ProcessorInstruction i4 = new MoveDocInstruction(MoveDocInstruction.DOWN);
        descriptor = NodeDescriptor.createNodeDescriptor("Items", null, AddNodeInstruction.ELEMENT, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i5 = new AddNodeInstruction(descriptor);
        ProcessorInstruction i6 = new MoveDocInstruction(MoveDocInstruction.DOWN);        

        ResultSetInfo info = new ResultSetInfo("xmltest.rs"); //$NON-NLS-1$
        ProcessorInstruction i7 = new ExecSqlInstruction("xmltest.rs", info);                 //$NON-NLS-1$
        ProcessorInstruction i8 = new MoveCursorInstruction("xmltest.rs"); //$NON-NLS-1$

        WhileInstruction i9 = new WhileInstruction("xmltest.rs");         //$NON-NLS-1$

        //need to move this up here so it can be referenced by "If" instruction
        ProcessorInstruction i17 = new MoveCursorInstruction("xmltest.rs"); //$NON-NLS-1$

        IfInstruction i10 = new IfInstruction( );
        descriptor = NodeDescriptor.createNodeDescriptor("Item", null, AddNodeInstruction.ELEMENT, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i11 = new AddNodeInstruction(descriptor);
        ProcessorInstruction i12 = new MoveDocInstruction(MoveDocInstruction.DOWN);
        descriptor = NodeDescriptor.createNodeDescriptor("ItemID", null, AddNodeInstruction.ATTRIBUTE, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i13 = new AddNodeInstruction(descriptor, new ElementSymbol("xmltest.rs.itemNum"));//$NON-NLS-1$
        descriptor = NodeDescriptor.createNodeDescriptor("Name", null, AddNodeInstruction.ELEMENT, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i14 = new AddNodeInstruction(descriptor, new ElementSymbol("xmltest.rs.itemName"));//$NON-NLS-1$
        descriptor = NodeDescriptor.createNodeDescriptor("Quantity", null, AddNodeInstruction.ELEMENT, null, null, null,false, null, MappingNodeConstants.NORMALIZE_TEXT_PRESERVE);//$NON-NLS-1$
        ProcessorInstruction i15 = new AddNodeInstruction(descriptor, new ElementSymbol("xmltest.rs.itemQuantity")); //$NON-NLS-1$       
        ProcessorInstruction i16 = new MoveDocInstruction(MoveDocInstruction.UP);

        ProcessorInstruction i18 = new MoveDocInstruction(MoveDocInstruction.UP);
        ProcessorInstruction i19 = new MoveDocInstruction(MoveDocInstruction.UP);
        ProcessorInstruction i20 = new MoveDocInstruction(MoveDocInstruction.UP);
        ProcessorInstruction i21 = new EndDocumentInstruction();

        
        // Stitch them together

        Program program = new Program();
        program.addInstruction(i0);
        program.addInstruction(i1);
        program.addInstruction(i2);
        program.addInstruction(i3);
        program.addInstruction(i4);
        program.addInstruction(i5);
        program.addInstruction(i6);
        program.addInstruction(i7);
        program.addInstruction(i8);
        program.addInstruction(i9);

        Program whileProgram = new Program();
        i9.setBlockProgram(whileProgram);
        whileProgram.addInstruction(i10);

        Program thenProgram = new Program();
        Condition cond = new CriteriaCondition(crit, thenProgram); 
        i10.addCondition(cond);

        thenProgram.addInstruction(i11);
        thenProgram.addInstruction(i12);
        thenProgram.addInstruction(i13);
        thenProgram.addInstruction(i14);
        thenProgram.addInstruction(i15);
        thenProgram.addInstruction(i16);

        whileProgram.addInstruction(i17);

        program.addInstruction(i18);
        program.addInstruction(i19);
        program.addInstruction(i20);
        program.addInstruction(i21);

        //ProgramUtil.printProgram(program);
        return program;        
    }
    
            
    public void testProcess1() throws Exception {
        FakeMetadataFacade metadata = exampleMetadata();
        String resultSetName = "xmltest.rs"; //$NON-NLS-1$
        
        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();
        Program program = exampleProgram(metadata, env);
        
        BufferManager bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
        XMLPlan temp = new XMLPlan(env);
        CommandContext context = new CommandContext("pid", null, null, null, 1); //$NON-NLS-1$
        temp.initialize(context,null,bufferMgr);

        List schema = new ArrayList();
        schema.add(new ElementSymbol(resultSetName + ElementSymbol.SEPARATOR + "itemNum")); //$NON-NLS-1$
        schema.add(new ElementSymbol(resultSetName + ElementSymbol.SEPARATOR + "itemName")); //$NON-NLS-1$
        schema.add(new ElementSymbol(resultSetName + ElementSymbol.SEPARATOR + "itemQuantity")); //$NON-NLS-1$
        
        env.addData(resultSetName, schema, new List[] { 
                    Arrays.asList( new Object[] { "001", "Lamp", new Integer(5) } ),         //$NON-NLS-1$ //$NON-NLS-2$
                    Arrays.asList( new Object[] { "002", "Screwdriver", new Integer(100) } ),         //$NON-NLS-1$ //$NON-NLS-2$
                    Arrays.asList( new Object[] { "003", "Goat", new Integer(4) } )         //$NON-NLS-1$ //$NON-NLS-2$
                    } );            
        helpProcessInstructions(program, env,  
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +  //$NON-NLS-1$
            "<Catalogs>\n" + //$NON-NLS-1$
            "   <Catalog>\n" +  //$NON-NLS-1$
            "      <Items>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"001\">\n" +  //$NON-NLS-1$
            "            <Name>Lamp</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>5</Quantity>\n" +  //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"002\">\n" +  //$NON-NLS-1$
            "            <Name>Screwdriver</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>100</Quantity>\n" +  //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"003\">\n" +  //$NON-NLS-1$
            "            <Name>Goat</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>4</Quantity>\n" +  //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "      </Items>\n" +  //$NON-NLS-1$
            "   </Catalog>\n" +  //$NON-NLS-1$
            "</Catalogs>"); //$NON-NLS-1$
    }
    
    public void testProcess2() throws Exception {
        FakeMetadataFacade metadata = exampleMetadata();

        String resultSetName = "xmltest.rs"; //$NON-NLS-1$
        
        String sql = "SELECT itemNum, itemName, itemQuantity FROM xmltest.rs";         //$NON-NLS-1$
        QueryCommand command = (QueryCommand) helpGetCommand(sql, metadata);               
        
        Criteria crit = helpGetCriteria("xmltest.rs.itemName = 'Screwdriver'", metadata);  //$NON-NLS-1$ 
        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();
        Program program = exampleProgram2(crit, metadata, env);
                
        BufferManager bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
        XMLPlan temp = new XMLPlan(env);
        CommandContext context = new CommandContext("pid", null, null, null, 1); //$NON-NLS-1$
        temp.initialize(context,null,bufferMgr);
        env.addData(resultSetName, command.getProjectedSymbols(), new List[] { 
                    Arrays.asList( new Object[] { "001", "Lamp", new Integer(5) } ),         //$NON-NLS-1$ //$NON-NLS-2$
                    Arrays.asList( new Object[] { "002", "Screwdriver", new Integer(100) } ),         //$NON-NLS-1$ //$NON-NLS-2$
                    Arrays.asList( new Object[] { "003", "Goat", new Integer(4) } )         //$NON-NLS-1$ //$NON-NLS-2$
                    } );            

        helpProcessInstructions(program, env, 
        	"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +  //$NON-NLS-1$
            "<Catalogs>\n" + //$NON-NLS-1$
            "   <Catalog>\n" +  //$NON-NLS-1$
            "      <Items>\n" +  //$NON-NLS-1$
            "         <Item ItemID=\"002\">\n" +  //$NON-NLS-1$
            "            <Name>Screwdriver</Name>\n" +  //$NON-NLS-1$
            "            <Quantity>100</Quantity>\n" +  //$NON-NLS-1$
            "         </Item>\n" +  //$NON-NLS-1$
            "      </Items>\n" +  //$NON-NLS-1$
            "   </Catalog>\n" +  //$NON-NLS-1$
            "</Catalogs>"); //$NON-NLS-1$
    }
}
