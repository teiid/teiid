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

package org.teiid.translator.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Function;

/**
 * Modifier for cases where arguments need to be rearranged
 * or new text needs to be added.
 *
 * E.g.
 *
 * new FunctionModifier() {
 *     public List<?> translate(Function function) {
 *         return Arrays.asList(
 *             "SDO_RELATE(",
 *             function.getParameters().get(0),
 *             ", ",
 *             function.getParameters().get(1),
 *             ", 'mask=touch')"
 *         );
 *     }
 * }
 *
 * new TemplateFunctionModifier("SDO_RELATE(", 0, ", ", 1, ", 'mask=touch')")
 *
 */
public class TemplateFunctionModifier extends FunctionModifier {
    private final Object[] parts;

    public TemplateFunctionModifier(Object... parts) {
        this.parts = parts;
    }

    @Override
    public List<?> translate(Function function) {
        List<Object> objs = new ArrayList<Object>();
        for (Object part : parts) {
            if (part instanceof Integer) {
                objs.add(function.getParameters().get((Integer) part));
            } else {
                objs.add(part);
            }
        }
        return objs;
    }

}
