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

package org.teiid.connector.jdbc.util;

import java.util.HashMap;
import java.util.Map;

import org.teiid.connector.jdbc.MetadataFactory;
import org.teiid.connector.jdbc.translator.DropFunctionModifier;
import org.teiid.connector.jdbc.translator.FunctionModifier;
import org.teiid.connector.jdbc.translator.ReplacementVisitor;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.visitor.framework.DelegatingHierarchyVisitor;

import junit.framework.TestCase;


/**
 */
public class TestFunctionReplacementVisitor extends TestCase {

    /**
     * Constructor for TestFunctionReplacementVisitor.
     * @param name
     */
    public TestFunctionReplacementVisitor(String name) {
        super(name);
    }

    public Map getModifierSet1() {
        Map modifiers = new HashMap();
        modifiers.put("concat", new DropFunctionModifier()); //$NON-NLS-1$ 
        modifiers.put("convert", new DropFunctionModifier()); //$NON-NLS-1$ 
        return modifiers;
        
    }
    
    public String getTestVDB() {
        return MetadataFactory.PARTS_VDB;
    }
    
    public void helpTestVisitor(String vdb, String input, final Map modifiers, String expectedOutput) {
        ICommand obj = MetadataFactory.helpTranslate(vdb, input);
        
        ReplacementVisitor visitor = new ReplacementVisitor(null, new Translator() {
        	@Override
        	public Map<String, FunctionModifier> getFunctionModifiers() {
        		return modifiers;
        	}
        });
        obj.acceptVisitor(new DelegatingHierarchyVisitor(null, visitor));
        
        //System.out.println(obj);               
        assertEquals("Did not get expected sql", expectedOutput, obj.toString()); //$NON-NLS-1$
    }
        
    public void testFunctionInSelect1() {        
        helpTestVisitor(getTestVDB(),
            "select concat(part_name, 'b') from parts",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS"); //$NON-NLS-1$
    }

    public void testFunctionInSelect2() {        
        helpTestVisitor(getTestVDB(),
            "select concat(part_name, 'b') AS x from parts",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME AS x FROM PARTS"); //$NON-NLS-1$
    }
    
    public void testFunctionInSelect3() {        
        helpTestVisitor(getTestVDB(),
            "select max(convert(part_name, string)) as x from parts",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT MAX(PARTS.PART_NAME) AS x FROM PARTS"); //$NON-NLS-1$
    }    

    public void testFunctionsNestedInSelect1() {        
        helpTestVisitor(getTestVDB(),
            "select concat(convert(part_name, string), 'b') AS x from parts",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME AS x FROM PARTS"); //$NON-NLS-1$
    }    

    public void testFunctionInCompare1() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where concat(part_name, 'b') = 'x'",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME = 'x'"); //$NON-NLS-1$
    }

    public void testFunctionInCompare2() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where 'x' = concat(part_name, 'b')",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME = 'x'"); //$NON-NLS-1$
    }
    
    public void testFunctionInSet1() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where concat(part_name, 'b') IN ('x')",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME = 'x'"); //$NON-NLS-1$
    }

    public void testFunctionInSet2() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where 'x' IN (concat(part_name, 'b'))",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME = 'x'"); //$NON-NLS-1$
    }

    public void testFunctionInIsNull() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where concat(part_name, 'b') IS NULL",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME IS NULL"); //$NON-NLS-1$
    }

    public void testFunctionInLike1() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where concat(part_name, 'b') Like 's%'",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME LIKE 's%'"); //$NON-NLS-1$
    }

    public void testFunctionInLike2() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where 's%' Like concat(part_name, 'b') ",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE 's%' LIKE PARTS.PART_NAME"); //$NON-NLS-1$
    }

    public void testFunctionInBetween() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where concat(part_name, 'b') between 'a' and 'f'",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE (PARTS.PART_NAME >= 'a') AND (PARTS.PART_NAME <= 'f')"); //$NON-NLS-1$
    }

    public void testNestedFunctionInCompare1() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where concat(convert(part_name, string), 'b') = 'x'",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME = 'x'"); //$NON-NLS-1$
    }

    public void testFunctionFromClauseJoin1() {        
        helpTestVisitor(getTestVDB(),
            "select X.part_name from parts X inner join parts Y on concat(X.part_name, 'b') = Y.part_name",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT X.PART_NAME FROM PARTS AS X INNER JOIN PARTS AS Y ON X.PART_NAME = Y.PART_NAME"); //$NON-NLS-1$
    }

    public void testFunctionFromClauseJoin2() {        
        helpTestVisitor(getTestVDB(),
            "select X.part_name from parts X inner join parts Y on concat(convert(X.part_name, string), 'b') = Y.part_name",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT X.PART_NAME FROM PARTS AS X INNER JOIN PARTS AS Y ON X.PART_NAME = Y.PART_NAME"); //$NON-NLS-1$
    }

    public void testFunctionInSelectSubquery1() {        
        helpTestVisitor(getTestVDB(),
            "select part_name, (select convert(part_name, string) from parts) as x from parts",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME, (SELECT PARTS.PART_NAME AS expr FROM PARTS) AS x FROM PARTS"); //$NON-NLS-1$
    }  
    
    public void testFunctionInSubquery1() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_name in (select convert(part_name, string) from parts) ",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME IN (SELECT PARTS.PART_NAME AS expr FROM PARTS)"); //$NON-NLS-1$
    }  

    public void testFunctionInSubquery2() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where exists (select convert(part_name, string) from parts) ",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE EXISTS (SELECT PARTS.PART_NAME AS expr FROM PARTS)"); //$NON-NLS-1$
    }    
    
    public void testFunctionInSubquery3() {        
        helpTestVisitor(getTestVDB(),
            "select part_name from parts where part_name <> some (select convert(part_name, string) from parts) ",  //$NON-NLS-1$
            getModifierSet1(), 
            "SELECT PARTS.PART_NAME FROM PARTS WHERE PARTS.PART_NAME <> SOME (SELECT PARTS.PART_NAME AS expr FROM PARTS)"); //$NON-NLS-1$
    }      
    
}

