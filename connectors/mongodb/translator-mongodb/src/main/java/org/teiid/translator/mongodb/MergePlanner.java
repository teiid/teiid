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
package org.teiid.translator.mongodb;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.teiid.translator.TranslatorException;

public class MergePlanner {
    protected LinkedHashSet<ProcessingNode> mergeProcessing = new LinkedHashSet<ProcessingNode>();
    
    public void addNode(ExistsNode node) throws TranslatorException {
        // only add exists node when no $unwind on that document, as unwind is implicit null check
        for (ProcessingNode pn: this.mergeProcessing) {
            if (pn instanceof UnwindNode) {
                if (pn.getDocumentName().equals(node.getDocumentName())) {
                    return;
                }
            }
        }
        this.mergeProcessing.add(node);
    }

    public void addNode(UnwindNode node)throws TranslatorException {
        // if there is a Exists node, remove it
        Iterator<ProcessingNode> iter = this.mergeProcessing.iterator();
        while (iter.hasNext()) {
            ProcessingNode pn = iter.next();
            if (pn instanceof ExistsNode) {
                if (pn.getDocumentName().equals(node.getDocumentName())) {
                    iter.remove();
                    break;
                }
            }
        }
        this.mergeProcessing.add(node);
    }
    
    public void addNode(ProjectionNode node, String alias) {
        for (ProcessingNode pn: this.mergeProcessing) {
            if (pn instanceof ProjectionNode) {
                ProjectionNode projectionNode = (ProjectionNode)pn;
                projectionNode.append(alias, node);
                return;
            }
        }
        
        this.mergeProcessing.add(node);
    }
    
    public Set<ProcessingNode> getNodes(){
        return this.mergeProcessing;
    }
}
