package multiQueryOptJoin.src.mqo_gq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.http.HTTPRepository;
import multiQueryOptJoin.src.Common.FullQuery;
import multiQueryOptJoin.src.Common.HittingSet;
import multiQueryOptJoin.src.Common.IntermediateResults;
import multiQueryOptJoin.src.Common.LocalQuery;
import multiQueryOptJoin.src.Common.Pair;
import multiQueryOptJoin.src.Common.RewrittenQuery;
import multiQueryOptJoin.src.Common.SeverInfo;
import multiQueryOptJoin.src.Common.TriplePattern;

/**
 * 优化Join
 * @author gq
 */
public class MultiQuery_gq_Hybrid {
	static HashMap<Integer,Set<String>> propertyMap = new HashMap<Integer,Set<String>>();
	public static ArrayList<FullQuery> query(String query) {
 		
		int templateNumArr = 1;
		
		//本地测试 - 
		String severPath = "/Users/zhiweixu/Desktop/FMQO/configure_lifeScience.txt"; //configure_peng_test.txt
		String severType = severPath.substring(severPath.lastIndexOf("/")+1,severPath.lastIndexOf("."));
		ArrayList<SeverInfo> severList = new ArrayList<SeverInfo>();
		String workloadPath = "";		 //D:/MQO/query/test_chebi.sparql  D:/MQO/query/query_50.txt
		String optimizaType = ""; // 设置优化类型 input: Filter , values ,optional or none
		String outputPath = "";	// 输出文件路径

		int windowSize = 2;
		int[] queryNumArr ={2}; 
		
		try {
			InputStream configIn = new FileInputStream(new File(severPath));
			Reader configInRead = new InputStreamReader(configIn);
			BufferedReader configInBufRead = new BufferedReader(configInRead);
			String str = configInBufRead.readLine();
			while (str != null && str.replace(" ", "").length()>0) {
//				System.out.println(str);
				String configName = str.split("=")[0];
				String configMain = str.split("=")[1];
				str = configInBufRead.readLine();
				if(configName.equals("DataServerFile")){  //数据库路径 DataServerFile
						InputStream in16;
						try {
							String serstr = "";
							String[] TermArr;	
							in16 = new FileInputStream(new File(configMain));
							Reader inr16 = new InputStreamReader(in16);
							BufferedReader br16 = new BufferedReader(inr16);
							serstr = br16.readLine();
							while (serstr != null) {
								serstr = serstr.trim();
								TermArr = serstr.split("\t");	
								severList.add(new SeverInfo(TermArr[0], TermArr[1]));	
								serstr = br16.readLine();
							}
							br16.close();
						} catch (Exception e1) {
							e1.printStackTrace();
						}	
				}
				else if(configName.equals("optimizaType")){ //优化类型
					optimizaType = configMain;
				}				
				else if(configName.equals("outputPath")){//输出路径
					outputPath = configMain;
				}	
				else if(configName.equals("WorkloadPath")){//查询路径
					workloadPath = configMain;
				}	
				else if(configName.equals("DataType")){
					severType = configMain;
				}
				else if(configName.equals("Data")){
					for(int fileId=0;fileId < configMain.split(";").length;fileId++){
						// 分别保存每个数据库的property
						String fileStr = configMain.split(";")[fileId];
						Set<String> proSet = new HashSet<String>();
						InputStream figIn = new FileInputStream(new File(fileStr));
						Reader figInRead = new InputStreamReader(figIn);
						BufferedReader figInBufRead = new BufferedReader(figInRead);
						String pstr = figInBufRead.readLine();
						while (pstr != null && pstr.replace(" ", "").length()>0) {
							proSet.add(pstr);
							pstr = figInBufRead.readLine();
						}
						// 保存到map
						for(int i=0;i<severList.size();i++){
							String ip = severList.get(i).getSesameServer().split(":")[1].substring(2);
//							System.out.println(ip);
//							if(fileStr.contains(ip)&&fileStr.contains(severList.get(i).getRepositoryID())){								
							if(fileStr.contains(severList.get(i).getRepositoryID())){						
								propertyMap.put(i, proSet);
							}
						}
//						System.out.println(propertyMap.size()+" , "+propertyMap.toString());
					}
				}
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		

		try {
			// 日志文件输出
			Date day = new Date();
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
			String currTime = df.format(day);
			int	query_n = queryNumArr[0];
            ArrayList<FullQuery> arrayList = new ArrayList<>();
			for (int i = 0; i < templateNumArr; i++) {
				PrintStream out1 = new PrintStream(new File(outputPath+"/"+query_n+"log_"+severType+"_"+optimizaType+"_"+ currTime+"_("+(i+1)+").txt"));
				String resultPath = outputPath+"/query"+ query_n + "_result" + currTime + "_("+(i+1)+").txt";
				// 第一次执行
                arrayList = run(severList, /*workloadPath,*/ query, resultPath, out1, query_n,optimizaType,windowSize);
				out1.println("templateNum =" + (i+1)+ ", queryNum =" + queryNumArr[0]);
				out1.println("severList : " + severList + "\n");
			
				System.out.println((i+1)+" is ok~");
				out1.flush();
				out1.close();
			}
			System.out.println("Done !!");
			return arrayList;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 
	 * @param severList
	 * @param SPARQLQuery
	 * @param resFileStr
	 * @param out1
	 * @param queryNum      
	 * @param optimizaType  优化类型
	 */
	public static ArrayList<FullQuery> run(ArrayList<SeverInfo> severList,/*String workloadFileStr,*/ String SPARQLQuery, String resFileStr, PrintStream out1,int queryNum,String optimizaType,int windowSize) {

		String str = "";
		int count = 0;
		try {
			// 保存全部查询
			ArrayList<FullQuery> allQueryList = new ArrayList<FullQuery>();

//			InputStream in6 = new FileInputStream(new File(workloadFileStr));
//			Reader inr6 = new InputStreamReader(in6);
//			BufferedReader br6 = new BufferedReader(inr6);

//			str = br6.readLine();
			int num = 0;
//			用"\n"分割得到各个查询语句
			String[] queries = SPARQLQuery.split("\n");
			str = queries[num];
			while (str != null && str.replace(" ", "").length()>0) {
				str = str.trim();
				allQueryList.add(new FullQuery(str));
				count++;
				if (count == queryNum)
					break;
				num++;
				str = queries[num];
//				str = br6.readLine();
			}
//			br6.close();
//			inr6.close();
//			in6.close();
			
			/*
			 * 查询预处理
			 * 1, 保存每个查询的三元组
			 * 2, 判断出每个三元组对应的数据库
			 * 
			 */
 			for(int q=0;q < queryNum;q++){
				FullQuery curFullQuery = allQueryList.get(q); // 当前查询
				SPARQLParser parser = new SPARQLParser();
				// 把查询转换为 triple pattern 的模式
				ParsedQuery query = parser.parseQuery(curFullQuery.getSPARQLStr(), null);
				StatementPatternCollector collector = new StatementPatternCollector();
				query.getTupleExpr().visit(collector);
				List<StatementPattern> patterns = collector.getStatementPatterns();

				int var_id = 0;
				String varStr = "";
				TreeMap<String, Integer> tmpVarIDMap = new TreeMap<String, Integer>();
				String curTriplePatternStr = "";
				/**
				 *  用自己封装的类来表示pattern, 遍历当前查询的每个triple pattern
				 */
				for (int i = 0; i < patterns.size(); i++) {
					// 当前三元组 -> StatementPattern
					StatementPattern curPattern = patterns.get(i);
					// 当前三元组 -> 自己定义的类 每个三元组里的情况，是否变量 常量
					TriplePattern myPattern = new TriplePattern();

					curTriplePatternStr = "";
					// 保存当前三元组的S部分
					if (!curPattern.getSubjectVar().isConstant()) {
						myPattern.setSubjectVarTag(true); // 当前三元组的 S 不是常量
						varStr = "?" + curPattern.getSubjectVar().getName();

						if (!tmpVarIDMap.containsKey(varStr)) {
							tmpVarIDMap.put(varStr, var_id);
							curFullQuery.getVarIDMap().put(varStr, var_id);
							curFullQuery.getIDVarMap().put(var_id, varStr);
							var_id++;
						}
						myPattern.setSubjectStr(varStr);
						curTriplePatternStr += varStr + "\t";
					} else {
						myPattern.setSubjectVarTag(false);
						curTriplePatternStr += "<"+ curPattern.getSubjectVar().getValue().toString() + ">\t";
						myPattern.setSubjectStr("<"+ curPattern.getSubjectVar().getValue().toString() + ">");
					}
					// 保存当前三元组的P部分
					if (!curPattern.getPredicateVar().isConstant()) {
						myPattern.setPredicateVarTag(true);
						varStr = "?"+ curPattern.getPredicateVar().getName();
						if (!tmpVarIDMap.containsKey(varStr)) {
							tmpVarIDMap.put(varStr, var_id);
							curFullQuery.getVarIDMap().put(varStr, var_id);
							curFullQuery.getIDVarMap().put(var_id, varStr);
							var_id++;
						}
						curTriplePatternStr += varStr + "\t";
						myPattern.setPredicateStr(varStr);
					} else {
						myPattern.setPredicateVarTag(false);
						curTriplePatternStr += "<"+ curPattern.getPredicateVar().getValue().toString() + ">\t";
						myPattern.setPredicateStr("<"+ curPattern.getPredicateVar().getValue().toString() + ">");
					}
					// 保存当前三元组的O部分
					if (!curPattern.getObjectVar().isConstant()) {
						myPattern.setObjectVarTag(true);
						varStr = "?" + curPattern.getObjectVar().getName();
						if (!tmpVarIDMap.containsKey(varStr)) {
							tmpVarIDMap.put(varStr, var_id);
							curFullQuery.getVarIDMap().put(varStr, var_id);
							curFullQuery.getIDVarMap().put(var_id, varStr);
							var_id++;
						}
						curTriplePatternStr += varStr + "\t";
						myPattern.setObjectStr(varStr);
					} else {
						myPattern.setObjectVarTag(false);
						String tmpConstantStr = curPattern.getObjectVar().getValue().toString();
						if (!tmpConstantStr.startsWith("\"")) {
							curTriplePatternStr += "<" + tmpConstantStr+ ">\t";
							myPattern.setObjectStr("<" + tmpConstantStr+ ">");
						} else {
							tmpConstantStr = tmpConstantStr.replace("^^<http://www.w3.org/2001/XMLSchema#string>","");
							curTriplePatternStr += tmpConstantStr + "\t";
							myPattern.setObjectStr(tmpConstantStr);
						}
					}
					curTriplePatternStr = curTriplePatternStr.trim();
					curTriplePatternStr += ".";
					curFullQuery.addTriplePattern(myPattern);
				}// pattern  end
//				curFullQuery.addLocalQueries(curLocalQueryList);
			}//查询预处理end ,判断并保存需要查询的语句与 服务器信息
			long startTime = System.currentTimeMillis();
			long preStartTime = System.currentTimeMillis();
 			TreeMap<String, ArrayList<Integer>> tpSourceMap = new TreeMap<String, ArrayList<Integer>>();
			/**
			 *  【判断当前 三元组存在于哪些数据库中】
			 *  需要：severList
			 *       myPattern 当前三元组模式 
			 *       curTriplePatternStr  当前三元组的字符串表示 
			 *       
			 *  输出：tpSourceMap 常量三元组 和 它对应的服务器
			 *       curSourceList  服务器资源列表
			 */
			// 服务器连接循环
/*    		for (int severId = 0; severId < severList.size(); severId ++) {
	 			SeverInfo tmpSeverInfo = severList.get(severId);
	 			Repository repo = new HTTPRepository(tmpSeverInfo.getSesameServer(),tmpSeverInfo.getRepositoryID());
				repo.initialize();
				RepositoryConnection con = repo.getConnection();
				// 查询循环
	 			for(int q=0;q < queryNum;q++){
					FullQuery curFullQuery = allQueryList.get(q); // 当前查询
					ArrayList<TriplePattern> curTriplePattern = curFullQuery.getQueryPattern().getTriplePatternList();
					// 三元组循环
					for(int t=0;t < curTriplePattern.size();t++){
							 ArrayList<Integer> curSourceList = new ArrayList<Integer>();
							 TriplePattern myPattern = curTriplePattern.get(t);
							 if (!tpSourceMap.containsKey(myPattern.getSignature())) {
								 // 初始每个pattern的资源列表为空
								 tpSourceMap.put(myPattern.getSignature(),curSourceList);
							 }else if(tpSourceMap.get(myPattern.getSignature()).contains(severId)){
									 continue;
							 }
							 String askQueryStr = "ask { "+ myPattern.toTriplePatternString() + "}";
							 BooleanQuery booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,askQueryStr);
							 boolean ask_res = booleanQuery.evaluate();
							 if (ask_res) {
									 curSourceList.add(severId);
									 if (tpSourceMap.containsKey(myPattern.getSignature())) {
										 if(!tpSourceMap.get(myPattern.getSignature()).contains(curSourceList.get(0))){
											// 当前常量 ，和它所在的数据库
											 tpSourceMap.get(myPattern.getSignature()).addAll(curSourceList);
										 } 
									 } 
							 }
					}
	 			}// q  for  end
				if (con.isOpen()) {
					con.close();
					repo.shutDown();
				}
			}// SERVER for end
*/  			
 			// 保存三元组模式
// 			int pcount = 0;
 			Set<TriplePattern> myPatternSet = new HashSet<TriplePattern>();
	 		for(int q=0;q < queryNum;q++){
				FullQuery curFullQuery = allQueryList.get(q); // 当前查询
				ArrayList<TriplePattern> curTriplePattern = curFullQuery.getQueryPattern().getTriplePatternList();
				for(int t=0;t < curTriplePattern.size();t++){
					 myPatternSet.add(curTriplePattern.get(t));
//					 pcount++;
				}
	 		}
	 	// 查询循环
/*  			for (int severId = 0; severId < severList.size(); severId ++) {
	 			SeverInfo tmpSeverInfo = severList.get(severId);
	 			Repository repo = new HTTPRepository(tmpSeverInfo.getSesameServer(),tmpSeverInfo.getRepositoryID());
				repo.initialize();
				RepositoryConnection con = repo.getConnection();
				for(TriplePattern myPattern : myPatternSet){
//					ArrayList<Integer> curSourceList = new ArrayList<Integer>();
					int curSourceList = severId;
					if (!tpSourceMap.containsKey(myPattern.getSignature())) {
							 // 初始每个pattern的资源列表为空
						 tpSourceMap.put(myPattern.getSignature(),new ArrayList<Integer>());		
					}else if(tpSourceMap.get(myPattern.getSignature()).contains(severId)){
						 continue;
					}

 					String askQueryStr = "ask { "+ myPattern.toTriplePatternString() + "}";
					BooleanQuery booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,askQueryStr);
					boolean ask_res = booleanQuery.evaluate(); 
					if (ask_res) {
//						curSourceList.add(severId);
						if (tpSourceMap.containsKey(myPattern.getSignature())) {
							if(!tpSourceMap.get(myPattern.getSignature()).contains(curSourceList)){
								// 当前常量 ，和它所在的数据库
								tpSourceMap.get(myPattern.getSignature()).add(curSourceList);
							} 
						} 
					}					
				}
				if (con.isOpen()) {
					con.close();
					repo.shutDown();
				}
 			} */

 	 		// 先直接通过 中间的property 保存出现的数据库
  			for(TriplePattern myPattern : myPatternSet){
//  				System.out.println(propertyMap);
				if (!tpSourceMap.containsKey(myPattern.getSignature())) {
						 // 初始每个pattern的资源列表为空
					 tpSourceMap.put(myPattern.getSignature(),new ArrayList<Integer>());		
				}
	 			Set<Integer> propertyKey = propertyMap.keySet();
	 			Iterator<Integer> propertyIter = propertyKey.iterator();
	 			while(propertyIter.hasNext()){
	 				int keyId = propertyIter.next();
	 				Set<String> propertySet = propertyMap.get(keyId);
//	 				System.out.println(keyId+" : "+propertySet.toString());
	 				// 遍历每个数据库的property
	 				if(propertySet.contains(myPattern.getPredicateStr())){
						if (tpSourceMap.containsKey(myPattern.getSignature())) {
							if(!tpSourceMap.get(myPattern.getSignature()).contains(keyId)){
								// 当前常量 ，和它所在的数据库
								tpSourceMap.get(myPattern.getSignature()).add(keyId);
							} 
						}	 					
	 				}
	 			}
 			} 
 			// 再把那些有常量的找出具体的服务器
  			for (int severId = severList.size()-1; severId >=0; severId--) {
	 			SeverInfo tmpSeverInfo = severList.get(severId);
	 			Repository repo = new HTTPRepository(tmpSeverInfo.getSesameServer(),tmpSeverInfo.getRepositoryID());
				repo.initialize();
				RepositoryConnection con = repo.getConnection();
				// 
 	 			for(TriplePattern myPattern : myPatternSet){
 	 				ArrayList<Integer> myPatternServer = tpSourceMap.get(myPattern.getSignature());
 	 				if(myPatternServer.size()!=0 && (!myPattern.isObjectVar() || !myPattern.isSubjectVar())){
 	 					if(myPatternServer.toString().contains(severId+"")){
 	 	 					String askQueryStr = "ask { "+ myPattern.toTriplePatternString() + "}";
 	 						BooleanQuery booleanQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL,askQueryStr);
 	 						boolean ask_res = booleanQuery.evaluate(); 
 	 						if (!ask_res) {
 	 							if(myPatternServer.indexOf(severId) != -1){
 	 								myPatternServer.remove(myPatternServer.indexOf(severId)); 
 	 							}
 	 						}
 	 					}
 	 				}
 	 			}	
 				if (con.isOpen()) {
					con.close();
					repo.shutDown();
				} 
 			} 

 			long preEndTime = System.currentTimeMillis();
			boolean isNoExist = false;		
 			// 保存每个查询的 localQueryList  
			for(int q=0;q < queryNum;q++){
				ArrayList<LocalQuery> curLocalQueryList = new ArrayList<LocalQuery>();
				FullQuery curFullQuery = allQueryList.get(q); // 当前查询
				ArrayList<TriplePattern> curTriplePattern = curFullQuery.getQueryPattern().getTriplePatternList();
				isNoExist = false;
				for(int t=0;t < curTriplePattern.size();t++){
					TriplePattern myPattern = curTriplePattern.get(t);
					ArrayList<Integer> curSourceList = tpSourceMap.get(myPattern.getSignature());
					// 如果当前常量 只存在一个数据库，则进行优化。。
					if (curSourceList.size() == 1) {
						int tag = 0;
						for (int k = 0; k < curLocalQueryList.size(); k++) {
							// curLocalQueryList中第K个查询triple和当前查询所在的数据库相同，并且都只有一个
							// ，就添加到curLocalQueryList的第K个的triple pattern。
							if (curLocalQueryList.get(k).getSourceList().equals(curSourceList)) {
								curLocalQueryList.get(k).addTriplePattern(myPattern);
								tag = 1;
								break;
							}
						}
						// curLocalQueryList 没有这样符合条件的查询triple
						if (0 == tag) {
							LocalQuery curLocalQuery = new LocalQuery();
							curLocalQuery.getSourceList().addAll(curSourceList);
							curLocalQuery.addTriplePattern(myPattern);
							curLocalQueryList.add(curLocalQuery);
						}
					} else if (curSourceList.size()>0){
						// 当前查询 triple 存在于多个数据库
						LocalQuery curLocalQuery = new LocalQuery();
						curLocalQuery.getSourceList().addAll(curSourceList);
						curLocalQuery.addTriplePattern(myPattern);
						curLocalQueryList.add(curLocalQuery);
					}else{
						isNoExist = true;break;
					}
				}
				if(!isNoExist){
					curFullQuery.addLocalQueries(curLocalQueryList);					
				}
			}

			int call_num = 0;
			// windowSize:每次执行的查询数
			for (int windowIdx = 0; windowIdx < queryNum / windowSize; windowIdx++) {
				// 比如：0-5,5-10 按段循环里面的每一个查询
				/**
				 *  oneEdgeHittingList ? 一条边 击中的查询
				 *  需要：allQueryList
				 */
				ArrayList<HittingSet> oneEdgeHittingList = new ArrayList<HittingSet>();
				for (int queryIdx = windowIdx*windowSize; queryIdx < windowSize*(windowIdx+1); queryIdx++) {
					// 遍历当前段 查询
					FullQuery curFullQuery = allQueryList.get(queryIdx);
					for (int j = 0; j < curFullQuery.getLocalQueryList().size(); j++) {
						LocalQuery curLocalQuery = curFullQuery.getLocalQuery(j);// 当前查询
						curLocalQuery.sort();
						// 当前查询对应的 三元组
						for (int k = 0; k < curLocalQuery.getTriplePatternList().size(); k++) {
							HittingSet tmpHittingSet = new HittingSet();
							// 初始化 -> 当前三元组  
							// HittingSet [triplePatternList=[TriplePattern [subjectStr=<http://bio2rdf.org/chebi:1>, predicateStr=?rv_0, objectStr=?rv_1]], HittingQuerySet=[]]
							tmpHittingSet.initializeTriplePattern(curLocalQuery.getTriplePatternList().get(k));
							int tmpIdx = oneEdgeHittingList.indexOf(tmpHittingSet);
//							boolean isConstantOne = false;
							if(oneEdgeHittingList.size()>0 && tmpIdx ==-1 && curLocalQuery.getTriplePatternList().size()==1){
								boolean tmpIsSubjectVar = tmpHittingSet.getTriplePatternList().get(0).isSubjectVar();
								boolean tmpIsObjectVar = tmpHittingSet.getTriplePatternList().get(0).isObjectVar();
								boolean tmpIsPredicate = tmpHittingSet.getTriplePatternList().get(0).isPredicateVar();
								String tmpIsPredicateVar = tmpHittingSet.getTriplePatternList().get(0).getPredicateStr();
								
								for(int edge=0;edge<oneEdgeHittingList.size() ;edge++){
									TriplePattern oneEdgeTriple = oneEdgeHittingList.get(edge).getTriplePatternList().get(0);
									if(oneEdgeHittingList.get(edge).getTriplePatternList().size() > 1){
										continue;
									}
									// 只有某多个查询中 ？x <h1> c1    ？x <h1> c2  这种查询才在这里处理.
									// 可以有多个一起优化，比如  ？x <h1> c1 ？x <h2> c2 同时在多个查询中出现, 试了一下，好复杂，调不出来，先不搞了
									int w1 = oneEdgeHittingList.get(edge).getHittingQuerySet().get(0).first;
									int w2 = oneEdgeHittingList.get(edge).getHittingQuerySet().get(0).second;
/*									int tmpSame = 0;
									ArrayList<TriplePattern> qTriples = allQueryList.get(w1).getLocalQuery(w2).getTriplePatternList();
									for(int i=0;i< qTriples.size();i++){
										for(int i2=0;i2 < curLocalQuery.getTriplePatternList().size();i2++){
											if(curLocalQuery.getTriplePatternList().get(i2).getPredicateStr().equals(qTriples.get(i).getPredicateStr())){
												tmpSame ++;
											}
										}
									}*/
									if(allQueryList.get(w1).getLocalQuery(w2).getTriplePatternList().size()>1){
										continue;
									}
									if((!tmpIsPredicate)&&(tmpIsSubjectVar==oneEdgeTriple.isSubjectVar()) 
											&& (tmpIsObjectVar==oneEdgeTriple.isObjectVar())&& (tmpIsPredicateVar.equals(oneEdgeTriple.getPredicateStr())) ){
										tmpIdx = edge;break;
										
									} 
								}
							} 
 
							if (-1 == tmpIdx) { // oneEdgeHittingList 中没有当前三元组的"击中信息"，则直接添加
								tmpHittingSet.addHittingPair(queryIdx, j); // 三元组 和 原始查询 对应  (第某个查询，0) 后面这个一般是0，干啥用
								oneEdgeHittingList.add(tmpHittingSet);
//								System.out.println("还没!!!!");
							} else { 
								// 这个三元组已经在之前出现过了,已经保存过它的三元组"击中信息"
								if (!oneEdgeHittingList.get(tmpIdx).hasBeenHit(queryIdx)) { 
									// 如果这个查询还没被击中，也就是，
									// 包含这个三元组的查询，还没保存到HitPair ，则把它添加到 HitPair
									oneEdgeHittingList.get(tmpIdx).addHittingPair(queryIdx, j);
//									System.out.println("还没被击中!!!!");
								} else { 
									// 击中这个三元组的查询已经保存过了
									tmpHittingSet.addHittingPair(queryIdx, j);
									oneEdgeHittingList.add(tmpHittingSet);
								}
							}			
						}
					}
				}
				
				/**
				 * - 开始重写查询 -
				 * 重写查询
				 * 需要：allQueryList，oneEdgeHittingList
				 * 得到：rewrittenQueryList
				 */
				ArrayList<RewrittenQuery> rewrittenQueryList = new ArrayList<RewrittenQuery>();
				sortByHittingQueryNum(oneEdgeHittingList, 0,oneEdgeHittingList.size());
				// group all local queries into different sets
				while (oneEdgeHittingList.size() > 0) {
					HittingSet tmpHittingSet = oneEdgeHittingList.get(0);
					if (tmpHittingSet.getHittingQuerySet().size() == 0)
						break;
					// rewrite local queries
					rewrittenQueryList.addAll(RewrittenQuery.rewriteQueries(allQueryList, tmpHittingSet,rewrittenQueryList.size()));

					//for (int j = 1; j < oneEdgeHittingList.size(); j++) {
					for (int j = 1; j < oneEdgeHittingList.size(); j++) {
						oneEdgeHittingList.get(j).removeQueries(tmpHittingSet);
						if(oneEdgeHittingList.get(j).getHittingQuerySet().size()==0){
							oneEdgeHittingList.remove(j);
						}
					}
					sortByHittingQueryNum(oneEdgeHittingList, 1,oneEdgeHittingList.size());
				}	
				/**
				 * 查询重写完毕~
				 * 执行重写的查询
				 * 查询结果保存在resultList中
				 */
				for (int queryIdx = 0; queryIdx < rewrittenQueryList.size(); queryIdx++) {
					System.out.println("============"+ " begin to find results of rewritten query 第"+ queryIdx + "个查询  in 第" + windowIdx + "个窗口=============");
					
					//每次找出score最大的，先执行
					RewrittenQuery curRewrittenQuery = rewrittenQueryList.get(queryIdx);
					int maxScore = -1;
					for (int q = 0; q < rewrittenQueryList.size(); q++) {
						if(rewrittenQueryList.get(q).getScore() > maxScore){
							maxScore = rewrittenQueryList.get(q).getScore();
							curRewrittenQuery = rewrittenQueryList.get(q);
						}
					}
					curRewrittenQuery.setScore(-1); // 将执行过的查询score设为-1
					HashSet<String> tmpMainVertexIDSet = new HashSet<String>(curRewrittenQuery.getMainPatternGraph().getVertexIDmap().keySet());
					
					// curRewrittenQueryStr  用SPARQL查询语句来表达 
					List<String> RewrittenQueryList = curRewrittenQuery.toSPARQLString();
					ArrayList<Integer> curSourceList = curRewrittenQuery.getSourceList(); 
					 
					// ?没有对应的资源，不会被执行
					out1.println("------------------------- 第["+queryIdx+"]个查询, ID =["+curRewrittenQuery.getRewrittenQueryID()+"], score=["+maxScore+"] -------------------------");
					out1.println();	
					for (int q = 0; q < RewrittenQueryList.size(); q++) {
						if(RewrittenQueryList.size()>1){
							out1.print("q"+queryIdx+""+(q+1)+" = ");
						}
						out1.println(RewrittenQueryList.get(q));
						out1.println();
					 }					
					
					if (curSourceList.size() == 0) { 
						    out1.println("This query don't hava a source, it would not be executed");	
							ArrayList<String> bindingNames = new ArrayList<String>();
							String tmpVarStr = "";
							for (int tpIdx = 0; tpIdx < curRewrittenQuery.getMainPatternGraph().getTriplePatternList().size(); tpIdx++) {
								TriplePattern curTriplePattern = curRewrittenQuery.getMainPatternGraph().getTriplePattern(tpIdx);
								if (curTriplePattern.isSubjectVar()&& !bindingNames.contains(curTriplePattern.getSubjectStr())) {
									tmpVarStr = curTriplePattern.getSubjectStr();
									tmpVarStr = tmpVarStr.substring(1);
									bindingNames.add(tmpVarStr);
								}
	
								if (curTriplePattern.isPredicateVar()&& !bindingNames.contains(curTriplePattern.getPredicateStr())) {
									tmpVarStr = curTriplePattern.getPredicateStr();
									tmpVarStr = tmpVarStr.substring(1);
									bindingNames.add(tmpVarStr); 
								}
	
								if (curTriplePattern.isObjectVar()&& !bindingNames.contains(curTriplePattern.getObjectStr())) {
									tmpVarStr = curTriplePattern.getObjectStr();
									tmpVarStr = tmpVarStr.substring(1);
									bindingNames.add(tmpVarStr);
								}
							}
							for (int bgpIdx = 0; bgpIdx < curRewrittenQuery.getOptionalPatternList().size(); bgpIdx++) {
								for (int tpIdx = 0; tpIdx < curRewrittenQuery.getOptionalPatternList().get(bgpIdx).getTriplePatternList().size(); tpIdx++) {
	
									TriplePattern curTriplePattern = curRewrittenQuery.getOptionalPatternList().get(bgpIdx).getTriplePattern(tpIdx);
									if (curTriplePattern.isSubjectVar()&& !bindingNames.contains(curTriplePattern.getSubjectStr())) {
										tmpVarStr = curTriplePattern.getSubjectStr();
										tmpVarStr = tmpVarStr.substring(1);
										bindingNames.add(tmpVarStr);
									}
	
									if (curTriplePattern.isPredicateVar()&& !bindingNames.contains(curTriplePattern.getPredicateStr())) {
										tmpVarStr = curTriplePattern.getPredicateStr();
										tmpVarStr = tmpVarStr.substring(1);
										bindingNames.add(tmpVarStr);
									}
	
									if (curTriplePattern.isObjectVar()&& !bindingNames.contains(curTriplePattern.getObjectStr())) {
										tmpVarStr = curTriplePattern.getObjectStr();
										tmpVarStr = tmpVarStr.substring(1);
										bindingNames.add(tmpVarStr);
									}
								}
							}
							curRewrittenQuery.addAllBindingNames(bindingNames);
							continue;
					} // curSourceList.size() == 0  判断 end
			
					// 开始查询   将当前重写的查询在它属于的各个数据库去查询
					int res_count=0;
					for (int k = 0; k < curSourceList.size(); k++) {
						
						// 连接数据库服务器
					  SeverInfo tmpSeverInfo = severList.get(curSourceList.get(k));
					  for (int q = 0; q < RewrittenQueryList.size(); q++) {
							Repository repo = new HTTPRepository(tmpSeverInfo.getSesameServer(),tmpSeverInfo.getRepositoryID());
							repo.initialize();
							RepositoryConnection con = repo.getConnection();
							call_num++;  //远程连接数
						 try{					
							String curRewrittenQueryStr = RewrittenQueryList.get(q);
							TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, curRewrittenQueryStr);	
							
								TupleQueryResult result = tupleQuery.evaluate();
								List<String> bindingNames = result.getBindingNames();
								if (k == 0 && q == 0 && !curRewrittenQuery.isRewriteOptional()) {
									curRewrittenQuery.addAllBindingNames(bindingNames); 
								}
		   					    if(curRewrittenQuery.isRewriteOptional()){
		   					    	// optional 优化，用
									bindingNames = curRewrittenQuery.getBindingNames();
								} 
								int cur_res_count = 0;
								String[] curResult = null;
								while (result.hasNext()) {
//		 	 							if (res_count == 15)
//		 									break; 
										res_count++;
										cur_res_count++;
										BindingSet bindingSet = result.next();
										
										if(!curRewrittenQuery.isRewriteOptional()){
											 curResult = new String[bindingNames.size()];
										}else{
											// optional 优化
											 curResult = new String[curRewrittenQuery.getBindingNames().size()];
										}
		//								String[] curResOfMainPattern = new String[bindingNames.size()];  //curResOfMainPattern 后面没用到
										int res_item_count = 0;
										// bindingNames :　查询中的变量名
										for (int i = 0; i < bindingNames.size(); i++) {
											String bandName = bindingNames.get(i);
											Value val_1 = bindingSet.getValue(bandName); // 当前变量的查询结果
											// 去除查询结果中string的尾缀
											String strTemp = "^^<http://www.w3.org/2001/XMLSchema#string>";
											if (val_1 != null) {
												if(!curRewrittenQuery.isRewriteOptional()){
													curResult[i] = val_1.toString().replace(strTemp, "");
//													out1.println(curResult[i]);
			/*										if (tmpMainVertexIDSet.contains("?"+ bindingNames.get(i))) {
														curResOfMainPattern[i] = curResult[i];
													}*/
												}
												else{// 当前重写查询是optional优化的，要改变量。根据变量的序号，去得到变量的值，再放入结果集
		//											System.out.println(curRewrittenQuery.getOptionalJoinList().size());
//													System.out.println("bandName : "+bandName);
													String varName = "?"+bandName.split("__")[0];
													int flag = Integer.parseInt(bandName.split("__")[1]);
													for(int b=0;b < curRewrittenQuery.getBindingNames().size();b++){
														if(varName.equals(curRewrittenQuery.getBindingNames().get(b))){
															curResult[b] = val_1.toString().replace(strTemp, "");
														}else{ //被optional覆盖的变量
															for(int opt=0;opt< curRewrittenQuery.getOptionalJoinList().size();opt++){
																String optVar = curRewrittenQuery.getOptionalJoinList().get(opt).first; 
																if(optVar.equals(curRewrittenQuery.getBindingNames().get(b))){
																	ArrayList<String> resultOptional = curRewrittenQuery.getOptionalJoinList().get(opt).second;
																	curResult[b] = resultOptional.get(flag).replace("<", "").replace(">", "");															
																}
															}	
														}
													}
													if(curResult!=null){
														curRewrittenQuery.addResult(curResult);
													}
												}
												res_item_count++;
											} else if(!curRewrittenQuery.isRewriteOptional()){ 
												curResult[i] = " ";
											}
										}
										// 变量名数量一致  或  存在结果的变量名和查询的变量名一致
										if(!curRewrittenQuery.isRewriteOptional()){
											if (curRewrittenQuery.getBindingNames().size() == tmpMainVertexIDSet.size()|| res_item_count != tmpMainVertexIDSet.size()) {
												// 重写列表中的  当前重写查询 添加结果
												curRewrittenQuery.addResult(curResult);
											}									
										}
									}//while  end
									if(RewrittenQueryList.size()>1){
										out1.print("q"+queryIdx+""+(q+1)+" : ");	
									}
									out1.println("共  "+cur_res_count+" 个结果，数据库是 "+tmpSeverInfo.getRepositoryID() +" , "+tmpSeverInfo.getSesameServer() );
									out1.println();
							  
							 }finally{
								 con.close();
								 repo.shutDown();							
							 }
						}
					 
					}// 当前查询在 所属资源中的查询结束
					out1.println("共  "+res_count+" 个结果");
					
					/**
					 *  优化后面的查询
					 *  前提：它们属于至少同一个查询，并且有相同的变量  ?x
					 *  过程：把 前查询的?x的结果 保存到  小查询的Filter中
					 */
					if(res_count> 0 && res_count < 10000 && (optimizaType.equals("Filter")|| optimizaType.equals("optional")|| optimizaType.equals("values"))){
//						int reCount = 0;
						for (int q = 0; q < rewrittenQueryList.size(); q++) {
							RewrittenQuery newReQuery = rewrittenQueryList.get(q);
							boolean isSameHit = false;
							boolean isSameVar = false;
							
							// 变量个数
							int varCount = newReQuery.getRenamedVarMapList().size();
							if(varCount>0){
								varCount = newReQuery.getRenamedVarMapList().get(0).size();
							}
//							System.out.println(newReQuery.getRenamedVarMapList().get(0).size());
//							System.out.println(newReQuery.getRenamedVarMapList().get(0).firstKey());
							// 
							if(newReQuery.getScore() == -1 || newReQuery.getSourceList().size() == 0 || varCount<2){ // 跳过 已查询过的
								continue;
							}
							// 判断是否属于相同查询,有可能同时属于多个查询
							Pair<Integer, Integer> sameHitPair = null;
							for(int h=0;h < curRewrittenQuery.getHittingQuerySet().size();h++){
								if(curRewrittenQuery.getHittingQuerySet().size() < newReQuery.getHittingQuerySet().size()){
									//当前查询击中的原始查询的数量 要 大于或等于 将要优化的新查询 才可以
									break;
								}
								for(int h2=0;h2 < newReQuery.getHittingQuerySet().size();h2++){
									if(curRewrittenQuery.getHittingQuerySet().get(h).first == newReQuery.getHittingQuerySet().get(h2).first){
										isSameHit = true;
										sameHitPair = curRewrittenQuery.getHittingQuerySet().get(h);
										break;
									}
								}	
								if(isSameHit)break;
							}
							String varRe = "";   // 当前执行查询中，相同的变量的重写String
							String varRe2 = "";  // 小查询中，相同的变量的重写String
							String varStr = "";  //相同的变量String
							int varId = 0;
							
							// 属于相同查询
							if(isSameHit){
								// 判断并找出相同变量，如果相同变量是主模式上的，进行判断，只取同一个原查询的结果
								for(int h=0;h < curRewrittenQuery.getOriginalVarMapList().size();h++){
										for(int h2=0;h2 < newReQuery.getOriginalVarMapList().size();h2++){ //小查询中原始查询 变量 循环
											for(int var2=0;var2 < newReQuery.getOriginalVarMapList().get(h2).size() ;var2 ++){
												String various = (String)newReQuery.getOriginalVarMapList().get(h2).keySet().toArray()[var2];
												if(curRewrittenQuery.getOriginalVarMapList().get(h).containsKey(various)){
													isSameVar = true;
													varStr = various;
													varRe = curRewrittenQuery.getOriginalVarMapList().get(h).get(various);
													varRe2 = newReQuery.getOriginalVarMapList().get(h2).get(various);
													break;
												}
											}
											if(isSameVar)break;
									}
									if(isSameVar)break;
								}
							}else{continue;}
							// 有相同变量
							if(isSameVar){
								// 主模式的变量
								String mainQueryVar = curRewrittenQuery.getMainPatternGraph().getVertexIDmap().keySet().toString();
								// optionan 里的变量，以及变量对应在bindname的位置
								
								boolean isMainVar = false;
								for(String idstr : curRewrittenQuery.getMainPatternGraph().getVertexIDmap().keySet()){
									if(idstr.equals(varRe)){
										isMainVar = true;break;
									}
								}
								
								int sameHitID = -1;
//								ArrayList<ArrayList<String>> varList = new ArrayList<ArrayList<String>>();
								ArrayList<ArrayList<Integer>> varIDList = new ArrayList<ArrayList<Integer>>();
								if(curRewrittenQuery.getOptionalPatternList().size()>1 && isMainVar){
									for(int h=0;h < curRewrittenQuery.getOptionalPatternList().size();h++){
	//									ArrayList<String> curVarList = new ArrayList<String>();
										ArrayList<Integer> curVarIDList = new ArrayList<Integer>();
										Set<String> curVarMap = curRewrittenQuery.getOptionalPatternList().get(h).getVertexIDmap().keySet();
										for(String vStr:curVarMap){
											if(!mainQueryVar.contains(vStr)){
	//											curVarList.add(vStr);
												for (int k = 0; k < curRewrittenQuery.getBindingNames().size(); k++) {
													if(curRewrittenQuery.getBindingNames().get(k).equals(vStr)){
														curVarIDList.add(k);
														break;
													}
												}
											}
										}
	//									varList.add(curVarList);
										varIDList.add(curVarIDList);
									}
									
									// 找出是和 第几个optional 匹配
									for(int h=0;h < curRewrittenQuery.getOptionalPatternList().size();h++){
										for(int h2=0;h2 < curRewrittenQuery.getOptionalPatternList().get(h).getTriplePatternList().size();h2++){
											String curPredict = curRewrittenQuery.getOptionalPatternList().get(h).getTriplePatternList().get(h2).getPredicateStr();
											ArrayList<TriplePattern> newLocalTriples = allQueryList.get(sameHitPair.first).getLocalQuery(sameHitPair.second).getTriplePatternList();
											for(int h3=0;h3< newLocalTriples.size();h3++ ){
												if(newLocalTriples.get(h3).getPredicateStr().equals(curPredict)){
													sameHitID = h;break;
												}
											}
											if(sameHitID!=-1)break;
										}
										if(sameHitID!=-1)break;
									}
								}
								// sameHitID  sameHitPair
								
								out1.println("相同的变量名： "+varStr+" , 匹配第"+newReQuery.getRewrittenQueryID()+"个查询");
								//获取变量ID
								for (int k = 0; k < curRewrittenQuery.getBindingNames().size(); k++) {
									if(curRewrittenQuery.getBindingNames().get(k).equals(varRe)){
										varId = k;
										break;
									}
								}
								
								int qSize = curRewrittenQuery.getResultList().size();
/* 								if(qSize>2000){
									qSize = 2000;
								} */
								// 把结果取出,去重
//								String[] sameVarResult = new String[qSize];
//								boolean isExit = false;
//								if(reCount ==0){
								Set<String> resultSet = new HashSet<String>();  
								int selfTmp = 0;
								for (int k = 0; k < qSize; k++) {
									selfTmp = 0;
									String varResult = curRewrittenQuery.getResultList().get(k)[varId]; //变量对应的某一条结果
/*									if(varResult.contains(":")&&!(varResult.contains("/"))){
										 continue;
									}*/
									if(curRewrittenQuery.getOptionalPatternList().size()>1 && isMainVar){
										for(int x2=0;x2<varIDList.get(sameHitID).size();x2++){
											// 当前变量对应的结果不为空
											if(curRewrittenQuery.getResultList().get(k)[varIDList.get(sameHitID).get(x2)].replace(" ", "").length()>0){
												selfTmp++;
											}
										}
 										if(selfTmp>0 &&varResult!=null && varResult.replace(" ","").length()>0){
											resultSet.add(varResult);
										} 
									}else{
 										if(varResult!=null && varResult.replace(" ","").length()>0){
											resultSet.add(varResult);
										}
									}
//										resultSet.add(varResult);
										// 判断是不是 当前原查询中的小查询
/*  										isExit = false;
										if(k==0){
											sameVarResult[k] = varResult;
											continue;
										}
		 								for(String sresult : sameVarResult){
											if(sresult!=null && sresult.replace(" ","").length()>0){
												if(sresult.equals(varResult)){
													isExit = true;
													break;
												}
											}
										}
										if(!isExit){
											sameVarResult[k] = varResult;
											// 保存 前一个查询结果中相关变量的结果
		//									out1.println(varResult);
										}  */
//									}
//									reCount ++;
								}
								String[] sameVarResult = new String[resultSet.size()];
								int rtmp =0;
								for(String rstr : resultSet){
									sameVarResult[rtmp++] = rstr;
								}
								
								// 用Filter 优化
								if(optimizaType.equals("Filter")){
										// 把结果添加到Filter去
										ArrayList<ArrayList<Pair<String, String>>> resultFilter = new ArrayList<ArrayList<Pair<String, String>>>();
										for(String sresult : sameVarResult){
												if(sresult!=null && sresult.replace(" ","").length()>0){
													if(sresult.contains("http") && sresult.contains("/")){
														sresult = "<"+sresult+">";
													}
													Pair<String, String>  resultPair = new Pair<String, String>(varRe2, sresult);
													ArrayList<Pair<String, String>> temp = new ArrayList<Pair<String, String>>();
													temp.add(resultPair);
													resultFilter.add(temp);
												}
										}
										// 把空的Filter删掉
										int FilterCount = 0;
										if (newReQuery.getFilterExpressionList().size() != 0) {
											// 如果是一个Filter的话，直接作为条件查询。多个的话，再改写为Filter
											for(int f0=0;f0<newReQuery.getFilterExpressionList().size();f0++){
												for(int f1 = 0;f1 <newReQuery.getFilterExpressionList().get(f0).size(); f1 ++){
													FilterCount += newReQuery.getFilterExpressionList().get(f0).get(f1).size();
												}	
												if(FilterCount == 0){
													newReQuery.getFilterExpressionList().remove(f0);
												}
											}
										}
										newReQuery.getFilterExpressionList().add(resultFilter);// 添加到Filter
										newReQuery.setIsRewriteFilter(true);
								}else if(optimizaType.equals("values")){
									// 用values 优化
									ArrayList<Pair<String, String>> resultValues = new ArrayList<Pair<String, String>>();
									String allResultStr = "";
									int temp = 0;
									for(int s=0;s<sameVarResult.length;s++){
										String sresult = sameVarResult[s];
										if(sresult!=null && sresult.replace(" ","").length()>0){
											if(sresult.contains("http") && sresult.contains("/")){
												sresult = "<"+sresult+">";
											}
											temp ++;
											allResultStr += (sresult+"\t");
										}
										if(temp%2000 ==0 || s==(sameVarResult.length-1)){
											Pair<String, String>  valuesPair = new Pair<String, String>(varRe2, allResultStr);
											resultValues.add(valuesPair);	
											allResultStr = "";
										}
//										System.out.println(s+" , "+allResultStr);
									}	
									newReQuery.setValuesList(resultValues); // 添加到重写列表的 valuesList 中
									newReQuery.setRewriteValues(true);
								}
								else if(optimizaType.equals("optional")){
									// 用 optional 优化
									ArrayList<String> resutltOptional = new ArrayList<String>();
									for(String sresult : sameVarResult){
										if(sresult!=null && sresult.replace(" ","").length()>0){
											if(sresult.contains("http") && sresult.contains("/")){
												sresult = "<"+sresult+">";
											}
											resutltOptional.add(sresult);
										}
									}
									Pair<String, ArrayList<String>> p = new Pair<String, ArrayList<String>>(varRe2, resutltOptional);
									ArrayList<Pair<String, ArrayList<String>>> optionalList = new ArrayList<Pair<String, ArrayList<String>>>();
									optionalList.add(p);

									newReQuery.setOptionalJoinList(optionalList);
									newReQuery.setRewriteOptional(true);
								}
								
								out1.println();
	//							System.out.println("varStr :"+varStr+","+varRe+","+varRe2);
	//							System.out.println(newReQuery.getFilterExpressionList().toString());
	//							System.exit(0);
							}
//						}
					}
				 }
				// curRewrittenQuery.distributeResultsInHittingQuery(allQueryList);
				}// 执行重写的查询  end
							
				
				if(rewrittenQueryList.size()==0){
					continue;
				}
				
				// 中间结果
				LinkedList<IntermediateResults> IntermediateResultsQueue = new LinkedList<IntermediateResults>();
				ArrayList<IntermediateResults> finalIntermediateResultsList = new ArrayList<IntermediateResults>();
				// 预先存重写列表的第一个
				IntermediateResultsQueue.add(new IntermediateResults(rewrittenQueryList.get(0)));
				rewrittenQueryList.remove(0);

				int queryIdx = 1; 
				while (rewrittenQueryList.size() != 0) {
						System.out.println("begin to " + queryIdx+ " th join ");
						queryIdx++;
	
						LinkedList<IntermediateResults> newIntermediateResultsList = new LinkedList<IntermediateResults>();
						/**
						 *  找出和当前查询队列中可Join的一个 重写查询
						 */
						int curIntermediateResultsID = findNextJoinableRewrittenQuery(IntermediateResultsQueue, rewrittenQueryList);
						
						IntermediateResults curIntermediateResults = null;
						
						if (curIntermediateResultsID != -1) {
							// 找到可以Join的重写查询，
//							System.out.println("找到可以Join的重写查询  id : "+rewrittenQueryList.get(curIntermediateResultsID).getRewrittenQueryID());
							curIntermediateResults = new IntermediateResults(rewrittenQueryList.get(curIntermediateResultsID));
							rewrittenQueryList.remove(curIntermediateResultsID);
						} else {
							// 没有可以Join的重写查询了，(也就是查询属于单个资源)，重新设置查询队列
							// 直接存 重写查询的结果
							finalIntermediateResultsList.addAll(IntermediateResultsQueue);// 保存上一个  相互连接的中间结果
							IntermediateResultsQueue.clear(); // 清空队列
							curIntermediateResults = new IntermediateResults(rewrittenQueryList.get(0)); // 初始当前第一个重写的查询
							IntermediateResultsQueue.add(curIntermediateResults);
							rewrittenQueryList.remove(0);
							continue;
						}
//						IntermediateResults curIntermediateResults2 = curIntermediateResults;
						while (IntermediateResultsQueue.size() != 0) {
							IntermediateResults preIntermediateResults = IntermediateResultsQueue.pollLast(); // 检索并移除此deque队列的最后(最高)一个元素
							// preIntermediateResults  ,curIntermediateResults是和第一个重写查询Join的查询
							newIntermediateResultsList.addAll(IntermediateResults.Join(preIntermediateResults,curIntermediateResults)); 
						} 
						//添加查询结果
//						System.out.println("HittingQuerySetGroup  : "+curIntermediateResults.getHittingQuerySetGroup().size()+" , "+curIntermediateResults.getHittingQuerySetGroup());
						if (curIntermediateResults.getHittingQuerySetGroup().size() != 0) {
							newIntermediateResultsList.add(curIntermediateResults);
						}
						IntermediateResultsQueue.addAll(newIntermediateResultsList);
				}// while  end
				 
				/* 把属于同一个查询的 重写查询分配到一起 */
				finalIntermediateResultsList.addAll(IntermediateResultsQueue);
				 
				// 分配结果
				for (int schemeIdx = 0; schemeIdx < finalIntermediateResultsList.size(); schemeIdx++) {
					System.out.println("+++++++++" + " begin to " + (schemeIdx+1)+ "th distribution " + "+++++++++");
					finalIntermediateResultsList.get(schemeIdx).distributeResultsInFullQuery(allQueryList);
				}
				
			}// 第一个(分段循环)  end

			long endTime = System.currentTimeMillis();

			System.out.println("----- FMQO -----");//OPTIONAL + union
			// 输出查询结果
			PrintStream resultOut = new PrintStream(new File(resFileStr));
			for (int queryIdx = 0; queryIdx < queryNum; queryIdx++) {
				FullQuery curFullQuery = allQueryList.get(queryIdx);
				for (int i = 0; i < curFullQuery.getResultList().size(); i++) {
					resultOut.print("[");
					for (int j = 0; j < curFullQuery.getResultList().get(i).length; j++) {
						resultOut.print(curFullQuery.getIDVarMap().get(j) + "="+ curFullQuery.getResultList().get(i)[j] + "\t");
					}
					resultOut.print("]");
					resultOut.println();
				}
				resultOut.println();resultOut.println();
				resultOut.println("==== there are "+ curFullQuery.getResultList().size()+ " results for query " + (queryIdx+1) + " ====");
				resultOut.println();resultOut.println();
			}
			out1.println("---------------------------------------------------------");
			
			out1.println("Source selection time = " + (preEndTime - preStartTime)+" (ms)");
			out1.println("evaluation time = " + (endTime - startTime)+" (ms)");
			out1.println("number of remote call = " + call_num);
			System.out.println("Source selection time = " + (preEndTime - preStartTime)+" (ms)");
			System.out.println("evaluation time = " + (endTime - startTime)+" (ms)");
			System.out.println("number of remote call = " + call_num);
//			System.out.println("end!!!");
			resultOut.flush();
			resultOut.close();
			return allQueryList;
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	    return null;
	}

	/**
	 * 和当前队列中  可以Join的查询
	 * 也就是，找出和当前重写查询  属于同一个原始查询的  重写查询
	 * @param intermediateResultsQueue  初始是第一个重写的查询
	 * @param rewrittenQueryList	        重写查询列表
	 * @return
	 */
	private static int findNextJoinableRewrittenQuery(LinkedList<IntermediateResults> intermediateResultsQueue,ArrayList<RewrittenQuery> rewrittenQueryList) {
		for (int i = 0; i < rewrittenQueryList.size(); i++) {
			RewrittenQuery curRewrittenQuery = rewrittenQueryList.get(i);
			// 查询列表的查询和中间结果。。。
			for (int j = 0; j < intermediateResultsQueue.size(); j++) {
				if (IntermediateResults.canJoin(intermediateResultsQueue.get(j), curRewrittenQuery)) {
//					System.out.println("ID : "+rewrittenQueryList.get(i).getRewrittenQueryID());
					return i;
				}
			}
		}
		return -1;
	}
	

	private static void sortByHittingQueryNum(ArrayList<HittingSet> oneEdgeHittingList, int min, int max) {
		TreeMap<Integer, ArrayList<HittingSet>> countMap = new TreeMap<Integer, ArrayList<HittingSet>>();
		int cur_count = 0;

		for (int i = min; i < max; i++) {
			HittingSet curHittingSet = oneEdgeHittingList.get(i);
			cur_count = curHittingSet.getHittingQuerySet().size();
			if (!countMap.containsKey(cur_count)) {
				countMap.put(cur_count, new ArrayList<HittingSet>());
			}
			countMap.get(cur_count).add(curHittingSet);
		}

		Iterator<Entry<Integer, ArrayList<HittingSet>>> iter = countMap.entrySet().iterator();
		oneEdgeHittingList.clear();
		while (iter.hasNext()) {
			Entry<Integer, ArrayList<HittingSet>> e = iter.next();
			ArrayList<HittingSet> curList = e.getValue();
			oneEdgeHittingList.addAll(0, curList);
		}
	}

}
