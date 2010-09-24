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

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.Assertion;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.xml.sax.SAXException;


/**
 */
public class MoveDocInstruction extends ProcessorInstruction {

    public static final int UP = 0;
    public static final int DOWN = 1;

    private int direction;

    /**
     * Constructor for MoveDocInstruction.
     */
    public MoveDocInstruction(int direction) {
        this.direction = direction;
    }

    /**
     * @see ProcessorInstruction#process(ProcessorEnvironment)
     */
    public XMLContext process(XMLProcessorEnvironment env, XMLContext context)
        throws BlockedException, TeiidComponentException, TeiidProcessingException{

        DocumentInProgress doc = env.getDocumentInProgress();

        switch(this.direction) {
            case UP:
                LogManager.logTrace(org.teiid.logging.LogConstants.CTX_XML_PLAN, "UP in document"); //$NON-NLS-1$
                try {
                    doc.moveToParent();
                } catch (SAXException err) {
                    throw new TeiidComponentException(err, "Failed to move UP in document");  //$NON-NLS-1$
                }
                break;
            case DOWN:
                LogManager.logTrace(org.teiid.logging.LogConstants.CTX_XML_PLAN, "LAST child in document"); //$NON-NLS-1$
                doc.moveToLastChild();
                break;
            default:
                Assertion.failed(QueryPlugin.Util.getString("ERR.015.006.0051", direction)); //$NON-NLS-1$
                break;
        }

        env.incrementCurrentProgramCounter();
        return context;
    }

    public String toString() {
        if(direction == UP) {
            return "UP"; //$NON-NLS-1$
        }
        return "LAST"; //$NON-NLS-1$
    }

    public PlanNode getDescriptionProperties() {
        if(direction == UP) {
            return new PlanNode("UP IN DOCUMENT"); //$NON-NLS-1$            
        } 
        return new PlanNode("NEXT IN DOCUMENT"); //$NON-NLS-1$
    }

}
