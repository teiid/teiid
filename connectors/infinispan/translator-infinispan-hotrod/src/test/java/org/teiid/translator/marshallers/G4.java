/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.marshallers;

public class G4 {
    int e1;
    String e2;

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
    @Override
    public String toString() {
        return "G4 [e1=" + e1 + ", e2=" + e2 + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        G4 other = (G4) obj;
        if (e1 != other.e1)
            return false;
        if (e2 == null) {
            if (other.e2 != null)
                return false;
        } else if (!e2.equals(other.e2))
            return false;
        return true;
    }
}
