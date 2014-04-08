package org.teiid.resource.adapter.simpledb;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.DomainMetadataResult;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
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

    @Test @Ignore
    public void getAttributeNamesEmptyStringTest() throws Exception {

        DomainMetadataResult metadataResult = mock(DomainMetadataResult.class);
        SelectResult result = mock(SelectResult.class);
        //		List itemsList = mock(ArrayList.class);
        //		Iterator iterator = mock(Iterator.class);
        //		Attribute
        //		when(itemsList.iterator()).thenReturn(iterator);
        when(metadataResult.getAttributeNameCount()).thenReturn(3);
        when(client.select(any(SelectRequest.class))).thenReturn(result);
        //		when(client.domainMetadata(any(DomainMetadataRequest.class))).thenReturn(metadataResult);

        System.out.println(simpleDbApi.getAttributeNames(null));
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
