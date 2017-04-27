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
package org.teiid.translator.marshallers;

import java.io.IOException;

import org.infinispan.protostream.MessageMarshaller;

public class G3Marshaller implements MessageMarshaller<G3> {

    @Override
    public String getTypeName() {
        return "pm1.G3";
    }

    @Override
    public Class<G3> getJavaClass() {
        return G3.class;
    }

    @Override
    public G3 readFrom(ProtoStreamReader reader) throws IOException {
        int e1 = reader.readInt("e1");
        String e2 = reader.readString("e2");

        G3 g3 = new G3();
        g3.setE1(e1);
        g3.setE2(e2);
        return g3;
    }

    @Override
    public void writeTo(ProtoStreamWriter writer, G3 g3) throws IOException {
        writer.writeInt("e1", g3.getE1());
        writer.writeString("e2", g3.getE2());
    }
}