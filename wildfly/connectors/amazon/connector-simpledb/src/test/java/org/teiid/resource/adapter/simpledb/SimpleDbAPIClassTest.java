package org.teiid.resource.adapter.simpledb;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.DomainMetadataRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

@SuppressWarnings("nls")
public class SimpleDbAPIClassTest {

    @Mock
    AmazonSimpleDBClient client;

    SimpleDBConnectionImpl simpleDbApi;

    @Before
    public void setUp(){
        MockitoAnnotations.initMocks(this);
        //I can't mock constructor of AmazonSimpleDBClient, thus this try/catch
        try{
            simpleDbApi = new SimpleDBConnectionImpl("test", "test");
        }catch(AmazonServiceException ex){
            //do nothing
        }
        replaceField("client", simpleDbApi, client);
    }

    @Test
    public void getDomainsTest() throws Exception {
        ListDomainsResult listDomainResult = Mockito.mock(ListDomainsResult.class);
        List<String> resultList = new ArrayList<String>();
        resultList.add("Test");
        resultList.add("Test1");
        when(client.listDomains()).thenReturn(listDomainResult);
        when(listDomainResult.getDomainNames()).thenReturn(resultList);
        assertEquals(resultList, simpleDbApi.getDomains());
    }

    @Test
    public void getAttributeNamesTest() throws Exception {

        DomainMetadataResult metadataResult = mock(DomainMetadataResult.class);
        SelectResult result = mock(SelectResult.class);
        ArrayList<Item> items = new ArrayList<Item>();
        items.add(new Item("1", Arrays.asList(new Attribute("c", "d"), new Attribute("a", "b"))));
        stub(result.getItems()).toReturn(items);
        when(metadataResult.getAttributeNameCount()).thenReturn(2);
        when(client.select(any(SelectRequest.class))).thenAnswer(new Answer<SelectResult>() {
            @Override
            public SelectResult answer(InvocationOnMock invocation)
                    throws Throwable {
                assertEquals("SELECT * FROM `x`", ((SelectRequest)invocation.getArguments()[0]).getSelectExpression());
                return result;
            }
        });
        when(client.domainMetadata(any(DomainMetadataRequest.class))).thenReturn(metadataResult);

        assertEquals("c", simpleDbApi.getAttributeNames("x").iterator().next().getName());
    }

    @Test
    public void testAddNullAttribute() throws Exception {
        ArrayList<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
        simpleDbApi.addAttribute("x", null, attributes);
        assertNull(attributes.get(0).getValue());
    }

    private void replaceField(String fieldName, Object object, Object newFieldValue){
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, newFieldValue);
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

}
