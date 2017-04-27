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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.protostream.MessageMarshaller;

public class G2Marshaller implements MessageMarshaller<G2> {

    @Override
    public String getTypeName() {
        return "pm1.G2";
    }

    @Override
    public Class<G2> getJavaClass() {
        return G2.class;
    }

    @Override
    public G2 readFrom(ProtoStreamReader reader) throws IOException {
        int e1 = reader.readInt("e1");
        String e2 = reader.readString("e2");
        G3 g3 = reader.readObject("g3", G3.class);
        List<G4> g4 = reader.readCollection("g4", new ArrayList<G4>(), G4.class);
        byte[] e5 = reader.readBytes("e5");
        long e6 = reader.readLong("e6");

        G2 g2 = new G2();
        g2.setE1(e1);
        g2.setE2(e2);
        g2.setG3(g3);
        g2.setG4(g4);
        g2.setE5(e5);
        g2.setE6(new Timestamp(e6));
        return g2;
    }

    @Override
    public void writeTo(ProtoStreamWriter writer, G2 g2) throws IOException {
        writer.writeInt("e1", g2.getE1());
        writer.writeString("e2", g2.getE2());
        writer.writeObject("g3", g2.getG3(), G3.class);
        writer.writeCollection("g4", g2.getG4(), G4.class);
        writer.writeBytes("e5", g2.getE5());
        writer.writeLong("e6", g2.getE6().getTime());
    }
}