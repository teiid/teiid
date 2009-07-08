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

package com.metamatrix.internal.core.index;

import org.teiid.internal.core.index.GammaCompressedIndexBlock;
import org.teiid.internal.core.index.IIndexConstants;
import org.teiid.internal.core.index.IndexBlock;
import org.teiid.internal.core.index.WordEntry;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestGammaCompressedIndexBlock extends TestCase {
    public TestGammaCompressedIndexBlock(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("TestGammaCompressedIndexBlock"); //$NON-NLS-1$
        suite.addTestSuite(TestGammaCompressedIndexBlock.class);
        return suite;
    }

    public void testAddAndRetrieveEntry() {
        IndexBlock indexBlock = new GammaCompressedIndexBlock(IIndexConstants.BLOCK_SIZE);
        WordEntry entry = new WordEntry();
        //adding entries - 256 chars
        char[] word = "12345678 abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn".toCharArray();//$NON-NLS-1$
        entry.reset(word);
        entry.addRef(1);
        indexBlock.addEntry(entry);
        
        word = "12345678 abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn abceabcdefghijklmn q".toCharArray();//$NON-NLS-1$
        entry.reset(word);
        entry.addRef(1);
        indexBlock.addEntry(entry);
        
        //reading entries
        indexBlock.reset();
        indexBlock.nextEntry(entry);
        indexBlock.nextEntry(entry);
    }
}
