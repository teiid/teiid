
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
 */package org.teiid.translator.swagger;

import java.util.List;

import org.teiid.metadata.ProcedureParameter;
import org.teiid.translator.document.Document;

public class SwaggerBodyInputDocument extends Document {

    public void addArgument(ProcedureParameter param, Object payload) {
        if (payload == null) {
            return;
        }
        String nis = param.getNameInSource();
        if (nis == null) {
            nis = param.getName();
        }
        addArgument(this, nis, payload);
    }

    private void addArgument(Document document, String nis,
            Object payload) {
        int index = nis.indexOf('/');
        if (index == -1) {
            if (payload instanceof Object[]) {
                Object[] array = (Object[])payload;
                for (Object anObj:array) {
                    document.addArrayProperty(nis, anObj);
                }
            } else {
                document.addProperty(nis, payload);
            }
        } else {
            String childPath = nis.substring(0, index);
            List<? extends Document> child = document.getChildDocuments(childPath);
            String propertyName = nis.substring(index+1);
            if (child == null) {
                boolean array = false;
                if (childPath.endsWith("[]")) {
                    childPath = childPath.substring(0, childPath.length()-2);
                    propertyName = nis.substring(nis.indexOf("/",index+1)+1);
                    array = true;
                }
                Document d  = new Document(childPath, array, this);
                child = document.addChildDocument(childPath, d);
            }
            addArgument(child.get(0), propertyName, payload);
        }
    }


}
