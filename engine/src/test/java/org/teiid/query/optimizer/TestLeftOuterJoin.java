package org.teiid.query.optimizer;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.relational.ListNestedSortComparator;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;

@Ignore
public class TestLeftOuterJoin {
	
	@Test public void testSomething() throws FileNotFoundException, IOException, Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertToString(new FileInputStream(UnitTestUtil.getTestDataFile("ddl.txt"))), "x", "y");
		String sql = "select evtsysid AS evtsysid, evtutctod AS evtutctod, evtsystod AS evtsystod, evtuserid AS evtuserid, rtrim(ltrim(name)) AS \"USEROFSYSACCESS@@@@NAME\", evtjobname AS evtjobname, evttypedesc AS evttypedesc, evtusername AS evtusername, evtcatdesc AS evtcatdesc, evtesmcode AS evtesmcode, convert(vio1code, integer), convert(vio2code, integer), usrfacility AS usrfacility, 'SECCM.VIEWSYSACCESS' AS \"__objecttype__\" FROM (SELECT g_0.EVTSYSID, g_0.EVTUSERID, g_0.EVTUTCTOD, g_0.EVTSYSTOD, g_0.EVTJOBNAME, g_0.EVTTYPEDESC, g_0.EVTUSERNAME, g_0.EVTCATDESC, g_0.EVTESMCODE, g_0.VIO1CODE, g_0.VIO2CODE, g_0.USRFACILITY FROM VIEWSYSACCESS g_0) AS VIEWSYSACCESS LEFT OUTER JOIN (SELECT g_0.LPAR, g_0.SYSID FROM CMXREF AS g_0) AS CMXREF on evtsysid=lpar LEFT OUTER JOIN (SELECT g_0.SYSID, g_0.USERID, g_0.NAME FROM BASEUSER g_0) AS BASEUSER ON BASEUSER.sysid=CMXREF.sysid and RTRIM(evtuserid)=BASEUSER.userid order by evtsysid, evtutctod "
				+ "option makedep CMXREF";
		
		BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
	   	caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
	   	caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
	   	caps.setCapabilitySupport(Capability.ARRAY_TYPE, true);
	   	//caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
	   	ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(caps));
		System.out.println(plan);
		
		HardcodedDataManager hdm = new HardcodedDataManager() {
			public org.teiid.common.buffer.TupleSource registerRequest(org.teiid.query.util.CommandContext context, org.teiid.query.sql.lang.Command command, String modelName, org.teiid.query.processor.RegisterRequestParameter parameterObject) throws TeiidComponentException {
				String sql = command.toString();
				try {
					String file = null;
					Criteria crit = null;
					OrderBy orderBy = ((Query)command).getOrderBy(); 
					Map<ElementSymbol, Integer> elements = new HashMap<ElementSymbol, Integer>();
					if (sql.equals("SELECT g_0.EVTSYSID, g_0.EVTUSERID, g_0.EVTUTCTOD, g_0.EVTSYSTOD, g_0.EVTJOBNAME, g_0.EVTTYPEDESC, g_0.EVTUSERNAME, g_0.EVTCATDESC, g_0.EVTESMCODE, g_0.VIO1CODE, g_0.VIO2CODE, g_0.USRFACILITY FROM y.VIEWSYSACCESS AS g_0") ||
							sql.equals("SELECT g_0.EVTSYSID AS c_0, g_0.EVTUSERID AS c_1, g_0.EVTUTCTOD AS c_2, g_0.EVTSYSTOD AS c_3, g_0.EVTJOBNAME AS c_4, g_0.EVTTYPEDESC AS c_5, g_0.EVTUSERNAME AS c_6, g_0.EVTCATDESC AS c_7, g_0.EVTESMCODE AS c_8, g_0.VIO1CODE AS c_9, g_0.VIO2CODE AS c_10, g_0.USRFACILITY AS c_11 FROM y.VIEWSYSACCESS AS g_0 ORDER BY c_0")) {
						file = "viewaccess-sorted.csv";
					} else if (sql.equals("SELECT g_0.SYSID, g_0.USERID, g_0.NAME FROM y.BASEUSER AS g_0") ||
							sql.equals("SELECT g_0.SYSID AS c_0, g_0.USERID AS c_1, g_0.NAME AS c_2 FROM y.BASEUSER AS g_0 ORDER BY c_0, c_1")) {
						file = "baseuser-sorted.csv";
					} else if (sql.equals("SELECT g_0.SYSID AS c_0, g_0.USERID AS c_1, g_0.NAME AS c_2 FROM y.BASEUSER AS g_0 WHERE (g_0.SYSID, g_0.USERID) IN (('DE29', ''), ('DE29', '*BYPASS*'), ('DE29', '*MISSING'), ('DE29', 'DEMO'), ('DE29', 'ENF'), ('DE29', 'HOGWA01'), ('DE29', 'JES2'), ('DE29', 'MASTER1'), ('DE29', 'NOOMVS'), ('DE29', 'OEDFLTG'), ('DE29', 'OEDFLTU'), ('DE29', 'OMVSKERN'), ('DE29', 'QACMGRL'), ('DE29', 'RODER01'), ('DE29', 'SYSVIEW'), ('DE29', 'TSO'), ('DE30', 'ACFSTCID'), ('DE30', 'AUDTST'), ('DE30', 'AUDTST1'), ('DE30', 'AUDTST10'), ('DE30', 'AUDTST11'), ('DE30', 'AUDTST12'), ('DE30', 'AUDTST13'), ('DE30', 'AUDTST14'), ('DE30', 'AUDTST15'), ('DE30', 'AUDTST16'), ('DE30', 'AUDTST17'), ('DE30', 'AUDTST18'), ('DE30', 'AUDTST19'), ('DE30', 'AUDTST2'), ('DE30', 'AUDTST20'), ('DE30', 'AUDTST23'), ('DE30', 'AUDTST3'), ('DE30', 'AUDTST4'), ('DE30', 'AUDTST5'), ('DE30', 'AUDTST6'), ('DE30', 'AUDTST7'), ('DE30', 'AUDTST9'), ('DE30', 'BPXAS'), ('DE30', 'CICTH01'), ('DE30', 'CMGRALRT'), ('DE30', 'DEFAULTU'), ('DE30', 'DEMO'), ('DE30', 'ECAADM'), ('DE30', 'ECAINF'), ('DE30', 'ECAINFV'), ('DE30', 'ECAMNT'), ('DE30', 'ECANCL'), ('DE30', 'ECARAL'), ('DE30', 'ECASCP'), ('DE30', 'ECATST'), ('DE30', 'ECAUSR'), ('DE30', 'FTPD'), ('DE30', 'GUJSA02'), ('DE30', 'HOGWA01'), ('DE30', 'HOGWA02'), ('DE30', 'JES2'), ('DE30', 'NATCO02'), ('DE30', 'NOLID'), ('DE30', 'REPTH02'), ('DE30', 'SYSVIEW'), ('DE30', 'TA5254'), ('DE30', 'TSO'), ('DE30', 'TUNEM01'), ('DE30', 'WENDE01')) ORDER BY c_0, c_1")) {
						file = "baseuser-sorted.csv";
						crit = ((Query)command).getCriteria();
						elements.put(new ElementSymbol("g_0.SYSID"), 0);
						elements.put(new ElementSymbol("g_0.USERID"), 1);
					} else {
						return super.registerRequest(context, command, modelName, parameterObject);
					}
					BufferedReader br = new BufferedReader(new FileReader(UnitTestUtil.getTestDataFile(file)));
					ArrayList<List<?>> tuples = new ArrayList<List<?>>();
					String line = null;
					List<Expression> cols = command.getProjectedSymbols();
					int lineNumber = 0;
					
					Evaluator eval = new Evaluator(elements, null, null);
					
					while ((line = br.readLine()) != null) {
						lineNumber++;
						ArrayList<Object> tuple = new ArrayList<Object>();
						String[] parts = line.split(",");
						int col = 0;
						for (int i = 0; i < parts.length; i++) {
							String part = parts[i];
							if (part.startsWith("\"")) {
								while (true) {
									part+=parts[i+1];
									i++;
									if (part.endsWith("\"")) {
										break;
									}
								}
								part = part.substring(1, part.length() -1);
							}
							Expression ex = cols.get(col);
							Class<?> type = ex.getType();
							tuple.add(DataTypeManager.transformValue(part, type));
							col++;
						}
						if (line.endsWith(",")) {
							tuple.add(null);
						}
						assertEquals(file + " " + lineNumber, cols.size(), tuple.size());
						if (crit != null) {
							if (!eval.evaluate(crit, tuple)) {
								continue;
							}
						}
						tuples.add(tuple);
					}
					if (orderBy != null) {
						int[] sortParameters =  new int[orderBy.getOrderByItems().size()];
						for (int i =0; i < sortParameters.length; i++) {
							sortParameters[i] = i;
						}
						ListNestedSortComparator comparator = new ListNestedSortComparator(sortParameters);
						Collections.sort(tuples, comparator);
					}
					return new CollectionTupleSource(tuples.iterator());

				} catch (Exception e) {
					throw new TeiidRuntimeException(e);
				}
			}
		};
		
		hdm.addData("SELECT g_0.LPAR AS c_0, g_0.SYSID AS c_1 FROM y.CMXREF AS g_0 WHERE g_0.LPAR IN ('DE29', 'DE30', 'DE31') ORDER BY c_0", Arrays.asList("DE29","DE29"), Arrays.asList("DE30","DE30"));
		hdm.addData("SELECT g_0.LPAR, g_0.SYSID FROM y.CMXREF AS g_0 WHERE g_0.LPAR IN ('DE29', 'DE30', 'DE31')", Arrays.asList("DE29","DE29"), Arrays.asList("DE30","DE30"));
		
		TestProcessor.doProcess(plan, hdm, null, TestProcessor.createCommandContext());
	}

}
