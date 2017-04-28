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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class G2 {
    int e1;
    String e2;

    G3 g3;
    List<G4> g4;

    byte[] e5;
    Timestamp e6;

    public int getE1() {
        return e1;
    }
    public void setE1(int e1) {
        this.e1 = e1;
    }
    public String getE2() {
        return e2;
    }
    public void setE2(String e2) {
        this.e2 = e2;
    }
    public G3 getG3() {
        return g3;
    }
    public void setG3(G3 g3child) {
        this.g3 = g3child;
    }
    public List<G4> getG4() {
        return g4;
    }
    public void setG4(List<G4> g4child) {
        this.g4 = g4child;
    }
    public byte[] getE5() {
        return e5;
    }
    public void setE5(byte[] bytes) {
        this.e5 = bytes;
    }
    public Timestamp getE6() {
        return e6;
    }
    public void setE6(Timestamp time) {
        this.e6 = time;
    }
    @Override
    public String toString() {
        return "G2 [e1=" + e1 + ", e2=" + e2 + ", g3=" + g3 + ", g4=" + g4 + ", bytes="
                + Arrays.toString(e5) + ", e6=" + e6 + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        G2 other = (G2) obj;
        if (!Arrays.equals(e5, other.e5))
            return false;
        if (e1 != other.e1)
            return false;
        if (e2 == null) {
            if (other.e2 != null)
                return false;
        } else if (!e2.equals(other.e2))
            return false;
        if (g3 == null) {
            if (other.g3 != null)
                return false;
        } else if (!g3.equals(other.g3))
            return false;
        if (g4 == null) {
            if (other.g4 != null)
                return false;
        } else if (!g4.equals(other.g4))
            return false;
        if (e6 == null) {
            if (other.e6 != null)
                return false;
        } else if (!e6.equals(other.e6))
            return false;
        return true;
    }


}
