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

package org.teiid.connector.jdbc.translator;

import java.util.List;

import org.teiid.connector.jdbc.JDBCPlugin;
import org.teiid.connector.language.*;


/**
 * This FunctionModifier will cause this function to be dropped by replacing the function
 * with (by default) the first argument of the function.  Optionally, the replacement index 
 * can be overridden.  This modifier should only be used with functions having the
 * minimum or more number of arguments. 
 */
public class DropFunctionModifier extends BasicFunctionModifier implements FunctionModifier {

    private int replaceIndex = 0;
    
    public void setReplaceIndex(int index) {
        this.replaceIndex = index;
    }
    
    public IExpression modify(IFunction function) {
        List<IExpression> args = function.getParameters();
        if(args.size() <= replaceIndex) { 
            throw new IllegalArgumentException(JDBCPlugin.Util.getString("DropFunctionModifier.DropFunctionModifier_can_only_be_used_on_functions_with___1") + function); //$NON-NLS-1$
        }

        return args.get(replaceIndex);
    }
}
