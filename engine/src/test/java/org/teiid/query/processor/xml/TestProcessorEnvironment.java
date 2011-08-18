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

import org.teiid.query.processor.xml.InitializeDocumentInstruction;
import org.teiid.query.processor.xml.Program;
import org.teiid.query.processor.xml.XMLContext;

import junit.framework.TestCase;

/**
 */
public class TestProcessorEnvironment extends TestCase {

    public TestProcessorEnvironment(String name) {
        super(name);
    }
    
    public void testProgramRecursionCount() {
        Program program = new Program();
        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();

        env.pushProgram(program);
        
        assertTrue(env.getProgramRecursionCount(program) == 0);
    }

    public void testProgramRecursionCount2() {
        Program program = new Program();
        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();

        env.pushProgram(program);
        env.pushProgram(program, true);
        
        assertTrue(env.getProgramRecursionCount(program) == 1);
    }
    
    public void testProgramRecursionCount3() {
        Program program = new Program();
        Program program2 = new Program();
        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();

        env.pushProgram(program);
        env.pushProgram(program2);
        env.pushProgram(program, true);
        
        assertTrue(env.getProgramRecursionCount(program) == 1);
        assertTrue(env.getProgramRecursionCount(program2) == 0);
    }    

    public void testProgramRecursionCount4() {
        Program program = new Program();
        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();
        assertTrue(env.getProgramRecursionCount(program) == 0);
    }
    
    /**
     * Test that sub programs pushed on top of a 
     * a recursive program "inherit" the recursive
     * program's recursion count. 
     */
    public void testProgramRecursionCount5() {
        Program program = new Program();
        Program program2 = new Program();
        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();

        env.pushProgram(program);
        env.pushProgram(program, true);
        env.pushProgram(program2);
        
        assertTrue(env.getProgramRecursionCount(program) == 1);
        assertTrue(env.getProgramRecursionCount(program2) == 0);
    }    
    
    
    /**
     * Tests getting each current instruction and executing it,
     * one by one, down a stack of Programs including one recursive
     * Program.  Implicit in this test is that the program counter
     * is separate from the two occurances of the p1 instance. 
     * @throws Exception
     */
    public void testGetCurrentInstruction() throws Exception {
        XMLContext context = new XMLContext();
        NoOpInstruction i1 = new NoOpInstruction();
        NoOpInstruction i2 = new NoOpInstruction();
        NoOpInstruction i3 = new NoOpInstruction();
        NoOpInstruction i4 = new NoOpInstruction();
        Program p1 = new Program();
        p1.addInstruction(i1);
        p1.addInstruction(i2);
        Program p2 = new Program();
        p2.addInstruction(i3);
        p2.addInstruction(i4);
        
        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();
        env.pushProgram(p1);
        env.pushProgram(p2);
        env.pushProgram(p1, true); //simulate recursion
        
        assertEquals(p1, env.getCurrentProgram());
        assertEquals(i1, env.getCurrentInstruction(null));
        assertEquals(p1, env.getCurrentProgram());
        i1.process(env, context);
        assertEquals(p1, env.getCurrentProgram());
        assertEquals(i2, env.getCurrentInstruction(null));
        assertEquals(p1, env.getCurrentProgram());
        i2.process(env, context);
        assertEquals(p2, env.getCurrentProgram());
        assertEquals(i3, env.getCurrentInstruction(null));
        assertEquals(p2, env.getCurrentProgram());
        i3.process(env, context);
        assertEquals(p2, env.getCurrentProgram());
        assertEquals(i4, env.getCurrentInstruction(null));
        assertEquals(p2, env.getCurrentProgram());
        i4.process(env, context);
        assertEquals(p1, env.getCurrentProgram());
        assertEquals(i1, env.getCurrentInstruction(null));
        assertEquals(p1, env.getCurrentProgram());
        i1.process(env, context);
        assertEquals(p1, env.getCurrentProgram());
        assertEquals(i2, env.getCurrentInstruction(null));
        assertEquals(p1, env.getCurrentProgram());
        i2.process(env, context);
        assertEquals(p1, env.getCurrentProgram());
        assertEquals(null, env.getCurrentInstruction(null));
        assertEquals(p1, env.getCurrentProgram());
    }
    
    /**
     * Tests getting each current instruction and executing it,
     * one by one, down a stack of Programs including one recursive
     * Program.  Implicit in this test is that the program counter
     * is separate from the two occurances of the p1 instance. 
     * @throws Exception
     */
    public void testGetCurrentInstruction2() throws Exception {
        XMLContext context = new XMLContext();
        NoOpInstruction i1 = new NoOpInstruction();
        NoOpInstruction i2 = new NoOpInstruction();
        NoOpInstruction i3 = new NoOpInstruction();
        NoOpInstruction i4 = new NoOpInstruction();
        Program p1 = new Program();
        p1.addInstruction(i1);
        p1.addInstruction(i2);
        Program p2 = new Program();
        p2.addInstruction(i3);
        p2.addInstruction(i4);
        
        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();
        env.pushProgram(p1);
        env.pushProgram(p1, true); //simulate recursion
        env.pushProgram(p2);
        
        assertEquals(p2, env.getCurrentProgram());
        assertEquals(i3, env.getCurrentInstruction(null));
        assertEquals(p2, env.getCurrentProgram());
        i3.process(env, context);
        assertEquals(p2, env.getCurrentProgram());
        assertEquals(i4, env.getCurrentInstruction(null));
        assertEquals(p2, env.getCurrentProgram());
        i4.process(env, context);
        assertEquals(p1, env.getCurrentProgram());
        assertEquals(i1, env.getCurrentInstruction(null));
        assertEquals(p1, env.getCurrentProgram());
        i1.process(env, context);
        assertEquals(p1, env.getCurrentProgram());
        assertEquals(i2, env.getCurrentInstruction(null));
        assertEquals(p1, env.getCurrentProgram());
        i2.process(env, context);
        assertEquals(p1, env.getCurrentProgram());
        assertEquals(i1, env.getCurrentInstruction(null));
        assertEquals(p1, env.getCurrentProgram());
        i1.process(env, context);
        assertEquals(p1, env.getCurrentProgram());
        assertEquals(i2, env.getCurrentInstruction(null));
        assertEquals(p1, env.getCurrentProgram());
        i2.process(env, context);
        assertEquals(p1, env.getCurrentProgram());
        assertEquals(null, env.getCurrentInstruction(null));
        assertEquals(p1, env.getCurrentProgram());
    }    
    
    /**
     * Test that doc instructions don't do anything while
     * recursion is going on. 
     */
    public void testInitializeDocInstruction() throws Exception {
        XMLContext context = new XMLContext();
        Program p0 = new Program();
        InitializeDocumentInstruction i1 = new InitializeDocumentInstruction();
        Program p1 = new Program();
        p1.addInstruction(i1);

        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();
        env.pushProgram(p0);
        env.pushProgram(p1);
        i1.process(env, context); // Should create new document

        assertNotNull(env.getDocumentInProgress());
    }

    /**
     * Test that doc instructions don't do anything while
     * recursion is going on. 
     */
    public void testInitializeDocInstruction2() throws Exception {
        XMLContext context = new XMLContext();
        Program p0 = new Program();
        InitializeDocumentInstruction i1 = new InitializeDocumentInstruction();
        Program p1 = new Program();
        p1.addInstruction(i1);

        FakeXMLProcessorEnvironment env = new FakeXMLProcessorEnvironment();
        env.pushProgram(p0);
        env.pushProgram(p1);
        env.pushProgram(p0, true); //simulate recursion
        env.pushProgram(p1);
        i1.process(env, context); // Shouldn't create new document

        assertNull(env.getDocumentInProgress());
    }    

}
