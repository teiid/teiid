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

import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingChoiceNode;
import org.teiid.query.mapping.xml.MappingCommentNode;
import org.teiid.query.mapping.xml.MappingCriteriaNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingRecursiveElement;
import org.teiid.query.mapping.xml.MappingSequenceNode;


/** 
 * 
 */
public class FakeXMLMetadata {
    
    public static MappingDocument docWithExcluded() {
        MappingDocument doc = new MappingDocument(true);
        MappingElement root = doc.addChildElement(new MappingElement("root")); //$NON-NLS-1$

        MappingElement n1 = root.addChildElement(new MappingElement("element1", "nis_element1")); //$NON-NLS-1$ //$NON-NLS-2$
        n1.setSource("mappingclass1"); //$NON-NLS-1$
        
        MappingElement n1r = n1.addChildElement(new MappingRecursiveElement("element1", "mappingclass1")); //$NON-NLS-1$ //$NON-NLS-2$
        n1r.setNameInSource("nis_element1"); //$NON-NLS-1$
        n1r.setExclude(true);
        MappingElement n1r2 = n1.addChildElement(new MappingRecursiveElement("element1", "mappingclass1")); //$NON-NLS-1$ //$NON-NLS-2$
        n1r2.addChildElement(new MappingElement("donotdelete", "nis_donotdelete")).setExclude(true); //$NON-NLS-1$ //$NON-NLS-2$
        
        
        MappingChoiceNode choice1 = root.addChoiceNode(new MappingChoiceNode());
        MappingCriteriaNode crit1 = choice1.addCriteriaNode(new MappingCriteriaNode("one==one", false)); //$NON-NLS-1$
        crit1.addChildElement(new MappingElement("c1_Criteria_1", "nis_c1_Criteria_1")); //$NON-NLS-1$ //$NON-NLS-2$

        MappingCriteriaNode crit2 = choice1.addCriteriaNode(new MappingCriteriaNode()); 
        crit2.addChildElement(new MappingElement("c1_Criteria_2", "nis_c1_Criteria_2")); //$NON-NLS-1$ //$NON-NLS-2$
        choice1.setExclude(true);

        MappingChoiceNode choice2 = root.addChoiceNode(new MappingChoiceNode());
        
        MappingCriteriaNode crit21 = choice2.addCriteriaNode(new MappingCriteriaNode("one==one", false)); //$NON-NLS-1$
        crit21.addChildElement(new MappingElement("c2_Criteria_1")); //$NON-NLS-1$

        MappingCriteriaNode crit22 = choice2.addCriteriaNode(new MappingCriteriaNode()); 
        crit22.addChildElement(new MappingElement("c2_Criteria_2")).setExclude(true); //$NON-NLS-1$
        
        MappingSequenceNode seq1 = root.addSequenceNode(new MappingSequenceNode());
        seq1.addChildElement(new MappingElement("seq1_element1", "nis_seq1_element1")) //$NON-NLS-1$ //$NON-NLS-2$
            .setExclude(true);
        MappingElement seq1_e2 = seq1.addChildElement(new MappingElement("seq1_element2", "nis_seq1_element2")); //$NON-NLS-1$ //$NON-NLS-2$
        seq1_e2.setNillable(true);
                
        MappingElement n2 = root.addChildElement(new MappingElement("element2")); //$NON-NLS-1$
        n2.addCommentNode(new MappingCommentNode("this is comment")); //$NON-NLS-1$
        MappingAttribute attr1 = new MappingAttribute("element2_attribute1"); //$NON-NLS-1$
        n2.addAttribute(attr1);        
        attr1.setExclude(true);
        
        MappingAttribute attr2 = new MappingAttribute("element2_attribute2", "nis_element2_attribute2"); //$NON-NLS-1$ //$NON-NLS-2$        
        n2.addAttribute(attr2);
        return doc;
    }
}
