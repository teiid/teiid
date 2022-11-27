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

package org.teiid.translator.google.api.result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Object represeting row in Google Spreadsheets.
 *
 *  TODO metadata should be somehow loaded from google spreadsheets so that the cell values are typed (integer, string etc)
 * @author fnguyen
 *
 */
public class SheetRow {
    private List<Object> row = new ArrayList<Object>();

    public SheetRow() {
    }

    public SheetRow(String [] row) {
        this.row = new ArrayList<Object>(Arrays.asList(row));
    }


    public void addColumn(Object s)    {
        row.add(s);
    }

    public List<Object> getRow(){
        return row;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((row == null) ? 0 : row.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SheetRow other = (SheetRow) obj;
        if (row == null) {
            if (other.row != null)
                return false;
        } else if (!row.equals(other.row))
            return false;
        return true;
    }
//
    @Override
    public String toString(){
        return row.toString();
    }


}
