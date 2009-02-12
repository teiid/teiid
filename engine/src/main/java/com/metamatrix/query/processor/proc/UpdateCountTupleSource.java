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

package com.metamatrix.query.processor.proc;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.sql.lang.Command;

public class UpdateCountTupleSource implements
                            TupleSource {

    private boolean hasNextTuple = true;
    private List nextTuple;

    public UpdateCountTupleSource(List tuple) {
        nextTuple = tuple;
    }

    public UpdateCountTupleSource(int count) {
        nextTuple = new ArrayList(1);
        nextTuple.add(new Integer(count));
    }

    public List getSchema() {
        return Command.getUpdateCommandSymbol();
    }

    public List nextTuple() throws MetaMatrixComponentException {
        if (hasNextTuple) {
            hasNextTuple = false;
            return nextTuple;
        }
        return null;
    }

    public void closeSource() throws MetaMatrixComponentException {
    }
}