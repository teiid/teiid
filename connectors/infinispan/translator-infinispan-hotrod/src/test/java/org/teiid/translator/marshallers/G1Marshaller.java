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

public class G1Marshaller implements MessageMarshaller<G1> {

    @Override
    public String getTypeName() {
        return "pm1.G1";
    }

    @Override
    public Class<G1> getJavaClass() {
        return G1.class;
    }

    @Override
    public G1 readFrom(ProtoStreamReader reader) throws IOException {
        int e1 = reader.readInt("e1");
        String e2 = reader.readString("e2");
        float e3 = reader.readFloat("e3");
        String[] e4 = reader.readArray("e4", String.class);
        String[] e5 = reader.readArray("e5", String.class);

        G1 g1 = new G1();
        g1.setE1(e1);
        g1.setE2(e2);
        g1.setE3(e3);
        if (e4.length > 0) {
            g1.setE4(e4);
        }
        if (e5.length > 0) {
            g1.setE5(e5);
        }
        return g1;
    }

    @Override
    public void writeTo(ProtoStreamWriter writer, G1 g1) throws IOException {
        writer.writeInt("e1", g1.getE1());
        writer.writeString("e2", g1.getE2());
        writer.writeFloat("e3", g1.getE3());
        writer.writeArray("e4", g1.getE4(), String.class);
        writer.writeArray("e5", g1.getE5(), String.class);
    }
}