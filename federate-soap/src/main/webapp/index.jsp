<%@ page import="com.metamatrix.common.util.WSDLServletUtil" %>

<html>
	<head>
		<title>MetaMatrix Web Service Utilities</title>
		<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
		<style type="text/css">
		<!--
			.MMSOAPTitle {  
				font-family: Arial, Helvetica, sans-serif;
				font-size: 18pt;
				font-style: normal;
				font-weight: bold				
			}
			
			.deprecated {  
				font-family: Arial, Helvetica, sans-serif;
				font-size: 12pt;
				font-style: italic;
				color: red;
				font-weight: bold				
			}
			
			a {
				visited : #666699; link: #666699; active: #666699;
				hover: #1E90FF; text-decoration: none; 
			}
}
			
		-->
		</style>
	</head>
	<body bgcolor="#FFFFFF">
  		<div align="center">
  		<p><table width="900" border="0" cellspacing="0" cellpadding="0">
  		  <tr bgcolor="#edf0f6">
	  		  <td width="300">
	  		  	<a href="http://www.redhat.com/metamatrix"><img src="images/mmlogo.gif" border="no" title=""></img></a>
	  		  </td>
	  		  <td width="600">
	  		  	<span class="MMSOAPTitle">Web Service Utilities</span>
	  		  </td>
  		  </tr>
  		   <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td colspan="2" class="MMSOAPTitle"><b><u>MetaMatrix Web Service WSDL URL Generator</u></b><td>		      
   		  </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td colspan="2"><b>VDBs that contain Web Service models expose WSDL when deployed to a MetaMatrix Server that describes the web service operations in those models. This form can be used to generate a parameterized WSDL URL for such a VDB.</b><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td><a href="wsdlurlframe.htm" target="_self" title="Web Service WSDL URL Generator">Web Service WSDL URL Generator</a><td>	
		      <td>&nbsp;<td>	
	      </tr>
   	      <tr>
			  <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      	      <tr>
		      <td colspan="2" class="MMSOAPTitle"><b><u>Discover MetaMatrix Web Services</u></b><td>		      
   		  </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td colspan="2"><b>To auto discover web service VDB WSDL URLs using predefined MetaMatrix server values, the MetaMatrix Server or cluster of MetaMatrix servers must be defined in the web.xml file of the MetaMatrix SOAP web application. Also, the defined WSDL user needs to be assigned the Admin.ReadOnlyAdmin role. This WSDL URL is more concise than the parameterized version, but is only valid with MetaMatrix SOAP web applications were the MetaMatrix server values have been defined.</b><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td><a href="../metamatrix-soap/servlet/DiscoverWSDLServlet" target="wsdlUrls" title="Discover MetaMatrix Web Services">Discover MetaMatrix Web Services</a><td>	
		      <td>&nbsp;<td>	
	      </tr>
   	      <tr>
			  <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>	      
	      <tr>
		      <td colspan="2" class="MMSOAPTitle"><b><u>SQL Query Web Service WSDL</u></b><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td colspan="2"><b>Click on the link below to view the SQL Query Web Service WSDL for this server.</b><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td align="left"><a target="_blank" href="<%= WSDLServletUtil.getSqlQueryWebServiceUrl(request.getServerName(), false) %>" title="SQL Query Web Service WSDL (Non-secure)">SQL Query Web Service WSDL (Non-secure)</a><td>	
   		      <td>&nbsp;<td>
	      </tr>	      
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td align="left"><a target="_blank" href="<%= WSDLServletUtil.getSqlQueryWebServiceUrl(request.getServerName(), true) %>" title="SQL Query Web Service WSDL (Secure SSL)">SQL Query Web Service WSDL (Secure SSL)</a><td>	
   		      <td>&nbsp;<td>
	      </tr>	      
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td colspan="2"><hr><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
<!--	      <tr>
		      <td colspan="2"><b>Click on the link below to view the Web Service Utilites page for the <span class="deprecated">DEPRECATED</span> SQL Query Web Service</b><td>		      
	      </tr>
	      <tr>
		      <td>&nbsp;<td>	
		      <td>&nbsp;<td>	
	      </tr>
	      <tr>
		      <td align="left" colspan="2"><a href="deprecated.jsp" alt="Deprecated SQL Query Web Service Utilities Page"><span class="deprecated">(DEPRECATED)</span> SQL Query Web Service Web Service Utilies Page</a><td>	
	      </tr>	
	      -->      
		  </table>  		
</div>
  	</body>
</html>
