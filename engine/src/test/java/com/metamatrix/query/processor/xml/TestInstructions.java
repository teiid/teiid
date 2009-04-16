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

package com.metamatrix.query.processor.xml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.mapping.xml.MappingNodeConstants;
import com.metamatrix.query.mapping.xml.ResultSetInfo;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.resolver.util.ResolverVisitor;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.unittest.FakeMetadataStore;
import com.metamatrix.query.util.CommandContext;

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
        QueryNode rsQuery = new QueryNode("xmltest.rs", "SELECT itemNum, itemName, itemQuantity FROM stock.items"); //$NON-NLS-1$ //$NON-NLS-2$
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
    
    private Command helpGetCommand(String sql, FakeMetadataFacade metadata) throws QueryParserException, QueryResolverException, MetaMatrixComponentException, QueryValidatorException {
        // parse
        QueryParser parser = new QueryParser();
        Command command = parser.parseCommand(sql);
        
        QueryResolver.resolveCommand(command, metadata);
        command = QueryRewriter.rewrite(command, null, metadata, null);
        
        return command;        
    }

    private Criteria helpGetCriteria(String sql, FakeMetadataFacade metadata) throws QueryParserException, QueryResolverException, MetaMatrixComponentException {
        QueryParser parser = new QueryParser();
        Criteria crit = parser.parseCriteria(sql);
   
        ResolverVisitor.resolveLanguageObject(crit, null, metadata);

        return crit;        
    }

	private void compareDocuments(String expectedDoc, String actualDoc) {
		StringTokenizer tokens1 = new StringTokenizer(expectedDoc, "\r\n"); //$NON-NLS-1$
		StringTokenizer tokens2 = new StringTokenizer(actualDoc, "\n");//$NON-NLS-1$
		while(tokens1.hasMoreTokens()){
			String token1 = tokens1.nextToken().trim();
			if(!tokens2.hasMoreTokens()){
				fail("XML doc mismatch: expected=" + token1 + "\nactual=none");//$NON-NLS-1$ //$NON-NLS-2$
			}
			String token2 = tokens2.nextToken().trim();
			assertEquals("XML doc mismatch: ", token1, token2); //$NON-NLS-1$
		}
		if(tokens2.hasMoreTokens()){
			fail("XML doc mismatch: expected=none\nactual=" + tokens2.nextToken().trim());//$NON-NLS-1$
		}
	}
	
    public List helpProcessInstructions(Program prog, XMLProcessorEnvironment env) throws Exception {
        int counter = 0;
        XMLContext context = new XMLContext();
        env.pushProgram(prog);

        DocumentInProgress doc = null;
        
        LinkedList resultDocs = new LinkedList();
        
        ProcessorInstruction inst = env.getCurrentInstruction();
        while (inst != null){

            try {
                
                context = inst.process(env, context);

                //code to check for end of document, start new one,
                doc = env.getDocumentInProgress();
                if (doc != null){
                    if (doc.isFinished()){
                        env.setDocumentInProgress(null);
                        String docString = new String(doc.getNextChunk(10));
                        resultDocs.addLast(docString);
                    }
                }                    
                
            } catch(BlockedException e) {
                
            }
            
            // Catch run away processes
            if(counter++ > 100) {
                break;
            }

            inst = env.getCurrentInstruction();
        }
        
        return resultDocs;
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
        CommandContext context = new CommandContext("pid", null, null, null, null); //$NON-NLS-1$
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
        List resultDocs = helpProcessInstructions(program, env);
        
        String actualDoc = (String)resultDocs.iterator().next();
        
        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<Catalogs>\r\n" + //$NON-NLS-1$
            "    <Catalog>\r\n" +  //$NON-NLS-1$
            "        <Items>\r\n" +  //$NON-NLS-1$
            "            <Item ItemID=\"001\">\r\n" +  //$NON-NLS-1$
            "                <Name>Lamp</Name>\r\n" +  //$NON-NLS-1$
            "                <Quantity>5</Quantity>\r\n" +  //$NON-NLS-1$
            "            </Item>\r\n" +  //$NON-NLS-1$
            "            <Item ItemID=\"002\">\r\n" +  //$NON-NLS-1$
            "                <Name>Screwdriver</Name>\r\n" +  //$NON-NLS-1$
            "                <Quantity>100</Quantity>\r\n" +  //$NON-NLS-1$
            "            </Item>\r\n" +  //$NON-NLS-1$
            "            <Item ItemID=\"003\">\r\n" +  //$NON-NLS-1$
            "                <Name>Goat</Name>\r\n" +  //$NON-NLS-1$
            "                <Quantity>4</Quantity>\r\n" +  //$NON-NLS-1$
            "            </Item>\r\n" +  //$NON-NLS-1$
            "        </Items>\r\n" +  //$NON-NLS-1$
            "    </Catalog>\r\n" +  //$NON-NLS-1$
            "</Catalogs>\r\n\r\n"; //$NON-NLS-1$
        
        //assertEquals("XML doc mismatch: ", expectedDoc, actualDoc); //$NON-NLS-1$
        compareDocuments(expectedDoc, actualDoc);
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
        CommandContext context = new CommandContext("pid", null, null, null, null); //$NON-NLS-1$
        temp.initialize(context,null,bufferMgr);
        env.addData(resultSetName, command.getProjectedSymbols(), new List[] { 
                    Arrays.asList( new Object[] { "001", "Lamp", new Integer(5) } ),         //$NON-NLS-1$ //$NON-NLS-2$
                    Arrays.asList( new Object[] { "002", "Screwdriver", new Integer(100) } ),         //$NON-NLS-1$ //$NON-NLS-2$
                    Arrays.asList( new Object[] { "003", "Goat", new Integer(4) } )         //$NON-NLS-1$ //$NON-NLS-2$
                    } );            

        List resultDocs = helpProcessInstructions(program, env);
        
        String actualDoc = (String)resultDocs.iterator().next();

        String expectedDoc = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +  //$NON-NLS-1$
            "<Catalogs>\r\n" + //$NON-NLS-1$
            "    <Catalog>\r\n" +  //$NON-NLS-1$
            "        <Items>\r\n" +  //$NON-NLS-1$
            "            <Item ItemID=\"002\">\r\n" +  //$NON-NLS-1$
            "                <Name>Screwdriver</Name>\r\n" +  //$NON-NLS-1$
            "                <Quantity>100</Quantity>\r\n" +  //$NON-NLS-1$
            "            </Item>\r\n" +  //$NON-NLS-1$
            "        </Items>\r\n" +  //$NON-NLS-1$
            "    </Catalog>\r\n" +  //$NON-NLS-1$
            "</Catalogs>\r\n\r\n"; //$NON-NLS-1$
        
        //assertEquals("XML doc mismatch: ", expectedDoc, actualDoc); //$NON-NLS-1$
        compareDocuments(expectedDoc, actualDoc);
    }
}
