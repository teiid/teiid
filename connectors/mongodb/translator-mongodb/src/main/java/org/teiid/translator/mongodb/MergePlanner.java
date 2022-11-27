/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
