package org.teiid.translator.google.api.metadata;


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Metadata about specific spreadsheet.
 *
 */
public class SpreadsheetInfo {
    private Map<String, Worksheet> worksheetByName = new HashMap<String,Worksheet>();

    public Worksheet createWorksheet(String name) {
        return this.createWorksheet(name, name);
    }

    public Worksheet createWorksheet(String name, String title) {
        Worksheet worksheet = new Worksheet(name, title);
        worksheetByName.put(name, worksheet);
        return worksheet;
    }

    public Collection<Worksheet> getWorksheets(){
        return Collections.unmodifiableCollection(worksheetByName.values());
    }

    public Worksheet getWorksheetByName(String worksheet){
        return worksheetByName.get(worksheet);
    }

}
