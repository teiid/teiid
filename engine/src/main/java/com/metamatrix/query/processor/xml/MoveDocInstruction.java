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

import java.util.HashMap;
import java.util.Map;

import org.xml.sax.SAXException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.util.ErrorMessageKeys;
import com.metamatrix.query.util.LogConstants;

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
        throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException{

        DocumentInProgress doc = env.getDocumentInProgress();

        switch(this.direction) {
            case UP:
                LogManager.logTrace(LogConstants.CTX_XML_PLAN, "UP in document"); //$NON-NLS-1$
                try {
                    doc.moveToParent();
                } catch (SAXException err) {
                    throw new MetaMatrixComponentException(err, "Failed to move UP in document");  //$NON-NLS-1$
                }
                break;
            case DOWN:
                LogManager.logTrace(LogConstants.CTX_XML_PLAN, "LAST child in document"); //$NON-NLS-1$
                doc.moveToLastChild();
                break;
            default:
                Assertion.failed(QueryExecPlugin.Util.getString(ErrorMessageKeys.PROCESSOR_0051, direction));
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

    public Map getDescriptionProperties() {
        Map props = new HashMap();
        if(direction == UP) {
            props.put(PROP_TYPE, "UP IN DOCUMENT"); //$NON-NLS-1$            
        } else {
            props.put(PROP_TYPE, "NEXT IN DOCUMENT"); //$NON-NLS-1$
        }

        return props;
    }

}
