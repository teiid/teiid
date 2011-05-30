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

package org.teiid.query.optimizer.xml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingSourceNode;
import org.teiid.query.mapping.xml.MappingVisitor;
import org.teiid.query.mapping.xml.Navigator;
import org.teiid.query.mapping.xml.ResultSetInfo;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.processor.xml.TestXMLProcessor;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;



/** 
 * 
 */
public class TestNameInSourceResolverVisitor extends TestCase {
    static HashMap infos = new HashMap();
    
    XMLPlannerEnvironment getEnv(String sql) throws Exception{
        QueryMetadataInterface metadata = TestXMLProcessor.exampleMetadataCached();
        Query query = (Query)TestXMLProcessor.helpGetCommand(sql, metadata); 

        Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroups(query, true);
        GroupSymbol group = groups.iterator().next();
        
        MappingDocument docOrig = (MappingDocument)metadata.getMappingNode(metadata.getGroupID(group.getName())); 
        MappingDocument doc = docOrig.clone(); 

        XMLPlannerEnvironment env = new XMLPlannerEnvironment(metadata);
        env.mappingDoc = doc;
        
        setResultInfo("xmltest.group.items", new DummyCommand("xmltest.group.items", new String[] {"itemNum", "itemName", "itemQuantity", "itemStatus"})); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        setResultInfo("xmltest.suppliers", new DummyCommand("xmltest.suppliers", new String[] {"supplierNum", "supplierName", "supplierZipCode", "itemNum"})); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        setResultInfo("xmltest.orders", new DummyCommand("xmltest.orders", new String[] {"orderNum", "orderDate", "orderQty", "orderStatus"})); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        
        return env;
    }
    
    private static void setResultInfo(String groupName, Command command) {
        ResultSetInfo rsInfo = new ResultSetInfo(groupName);
        rsInfo.setCommand(command);
        infos.put(groupName, rsInfo);
    }
    
    private static ResultSetInfo getResultInfo(String groupName) {
        return (ResultSetInfo)infos.get(groupName);
    }
    
    public void testResolve() throws Exception {
        XMLPlannerEnvironment env = getEnv("SELECT * FROM xmltest.doc9"); //$NON-NLS-1$
        MappingDocument doc = env.mappingDoc;
        
        // we should expect to fail
        doc.acceptVisitor(new NameValidator(false));
        
        doc = SourceNodeGenaratorVisitor.extractSourceNodes(doc);
        doc.acceptVisitor(new Navigator(true, new SourceFixer()));
        NameInSourceResolverVisitor.resolveElements(doc, env);
        
        // now we pass
        doc.acceptVisitor(new NameValidator(true));        
    }
    
    
    static class SourceFixer extends MappingVisitor{
        public void visit(MappingSourceNode sourceNode) {
            ResultSetInfo info = getResultInfo(sourceNode.getResultName());
            Map symbolMap = new HashMap();
            for (Iterator i = info.getCommand().getProjectedSymbols().iterator(); i.hasNext();) {
                ElementSymbol element = (ElementSymbol)i.next();
                symbolMap.put(element, element);
            }
            sourceNode.setSymbolMap(symbolMap);
            sourceNode.setResultSetInfo(info);
        }
    }
    
    static class NameValidator extends MappingVisitor{
        boolean shouldPass;
        
        public NameValidator(boolean pass) {
            this.shouldPass = pass;
        }
        
        public void visit(MappingAttribute attribute) {
            if (attribute.getNameInSource() != null) {
                if (this.shouldPass) {
                    assertNotNull(attribute.getElementSymbol());
                }
                else {
                    assertNull(attribute.getElementSymbol());
                }
            }
        }

        public void visit(MappingElement element) {
            if (element.getNameInSource() != null) {
                if (this.shouldPass) {
                    assertNotNull(element.getElementSymbol());
                }
                else {
                    assertNull(element.getElementSymbol());
                }
            }
        }
    }
    
    static class DummyCommand extends Command{
        List list = new ArrayList();

        DummyCommand(String groupName, String[] symbols){
            for (int i= 0; i < symbols.length; i++) {
                list.add(new ElementSymbol(groupName+"."+symbols[i])); //$NON-NLS-1$
            }
        }
        public List getProjectedSymbols() {
            return list;
        }

        public boolean areResultsCachable() {
            return false;
        }

        public Object clone() {
            return null;
        }

        public int getType() {
            return 0;
        }

        public void acceptVisitor(LanguageVisitor visitor) {
        }        
    }
}
