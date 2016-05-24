package org.teiid.sizing;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestSizingApplication {
    
    private Caculation tool = null;
    
    @Test
    public void testHeapCacualtion_1() {
        tool = new Caculation(1, 200);
        assertEquals(2, tool.heapCaculation());
    }
    
    @Test
    public void testHeapCacualtion_2() {
        tool = new Caculation(2, 200);
        assertEquals(3, tool.heapCaculation());
    }
    
    @Test
    public void testHeapCacualtion_3() {
        tool = new Caculation(4, 200);
        assertEquals(5, tool.heapCaculation());
    }
    
    @Test
    public void testHeapCacualtion_4() {
        tool = new Caculation(5, 200);
        assertEquals(6, tool.heapCaculation());
    }
    
    @Test
    public void testHeapCacualtion_5() {
        tool = new Caculation(1, 300);
        assertEquals(2, tool.heapCaculation());
    }
    
    @Test
    public void testHeapCacualtion_6() {
        tool = new Caculation(2, 300);
        assertEquals(4, tool.heapCaculation());
    }
    
    @Test
    public void testHeapCacualtion_7() {
        tool = new Caculation(4, 300);
        assertEquals(7, tool.heapCaculation());
    }
    
    @Test
    public void testHeapCacualtion_8() {
        tool = new Caculation(5, 300);
        assertEquals(8, tool.heapCaculation());
    }
    
    @Test
    public void testHeapCacualtion_9() {
        tool = new Caculation(1, 400);
        assertEquals(3, tool.heapCaculation());
    }
    
    @Test
    public void testHeapCacualtion_10() {
        tool = new Caculation(2, 400);
        assertEquals(5, tool.heapCaculation());
    }
    
    @Test
    public void testHeapCacualtion_11() {
        tool = new Caculation(4, 400);
        assertEquals(9, tool.heapCaculation());
    }
    
    @Test
    public void testHeapCacualtion_12() {
        tool = new Caculation(5, 400);
        assertEquals(11, tool.heapCaculation());
    }
    
    @Test
    public void testCoreSizeCaculation_1() {
        tool = new Caculation(2, 500, 100, 10000, 100, 1000, 10000, 1000, 2000, false);
        assertEquals(27, tool.coreCaculation());
    }
    
    @Test
    public void testCoreSizeCaculation_2() {
        tool = new Caculation(2, 500, 100, 10000, 100, 500, 10000, 1000, 2000, false);
        assertEquals(58, tool.coreCaculation());
    }
    
    @Test
    public void testCoreSizeCaculation_3() {
        tool = new Caculation(2, 400, 200, 10000, 100, 500, 10000, 1000, 2000, true);
        assertEquals(207, tool.coreCaculation());
    }
    
    @Test
    public void testCoreSizeCaculation_4() {
        tool = new Caculation(2, 500, 500, 10000, 100, 500, 10000, 1000, 2000, false);
        assertEquals(290, tool.coreCaculation());
    }

}
