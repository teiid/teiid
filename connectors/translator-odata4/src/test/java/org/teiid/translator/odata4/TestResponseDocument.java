package org.teiid.translator.odata4;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class TestResponseDocument {

    private Map<String, Object> map(String... items){
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < items.length; i+=2) {
            map.put(items[i], items[i+1]);
        }
        return map;
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testFlatten() {
        ODataResponseDocument doc = new ODataResponseDocument();
        doc.addProperty("A", "AA");
        doc.addProperty("B", "BB");
        
        ODataResponseDocument c1 = new ODataResponseDocument("c1", doc);
        c1.addProperty("1", "11");
        ODataResponseDocument c2 = new ODataResponseDocument("c1", doc);
        c2.addProperty("2", "22");
        
        doc.addChildDocuments(Arrays.asList(c1, c2));
        
        ODataResponseDocument c4 = new ODataResponseDocument("c2", doc);
        c4.addProperty("4", "44");
        ODataResponseDocument c5 = new ODataResponseDocument("c2", doc);
        c5.addProperty("5", "55");
        doc.addChildDocuments(Arrays.asList(c4, c5));
                
        List<Map<String, Object>> result = doc.flatten();
        System.out.println(result);
        
        List<Map<String, Object>> expected = Arrays.asList(
                map("A", "AA", "B", "BB", "c1/1", "11", "c2/4","44"), 
                map("A", "AA", "B", "BB", "c1/2", "22", "c2/4", "44"),
                map("A", "AA", "B", "BB", "c1/1","11", "c2/5", "55"),                
                map("A", "AA", "B", "BB", "c1/2", "22", "c2/5", "55"));
        assertArrayEquals(expected.toArray(), result.toArray());
        
    }
}
