package Common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

/**
 * 
 * @author gq
 *
 */
public class RewrittenQuery {

	private int RewrittenQueryID;
	private BGPGraph MainPatternGraph;
	private ArrayList<BGPGraph> OptionalPatternList;
	private ArrayList<Pair<Integer, Integer>> HittingQuerySet;
	private ArrayList<Integer> sourceList;
	private ArrayList<String> bindingNames;
	private ArrayList<String[]> resultList;
	private int score = 0;  // 重写查询中 Filter 和 OPTIONAL 的个数
	private boolean isRewriteFilter = false;  // 是否为Filter 优化 join的 查询
	private boolean isRewriteValues = false;  // 是否为values 优化 join的 查询
	private boolean isRewriteOptional = false;  // 是否为values 优化 join的 查询
	private ArrayList<Pair<String, String>> ValuesList; 
	private ArrayList<Pair<String, ArrayList<String>>> OptionalJoinList; 
	// private ArrayList<Result> resultListOfMainPattern;
	// private ArrayList<ArrayList<String[]>> resultListOfHittingQuery;

	// The first element in a pair of FilterExpressionList is the variable in
	// the filter expression;
	// The second element in a pair of FilterExpressionList is the value in
	// the filter expression;
	// Each pair maps to an expression.
	// A list of pairs is the conjunction of expressions, and each list is
	// associated with a optional pattern.
	// Since a rewritten query may have multiple optional patterns, a rewritten
	// query may have multiple lists.
	private ArrayList<ArrayList<ArrayList<Pair<String, String>>>> FilterExpressionList;
    //                    或            且
	// each list maps to a local query
	private ArrayList<ArrayList<Pair<String, String>>> ConstraintList;

	// mapping from old variable name to new variable name
	private ArrayList<TreeMap<String, String>> OriginalVarMapList;
	// mapping from new variable name to old variable name
	private ArrayList<TreeMap<String, String>> RenamedVarMapList;

	public static ArrayList<RewrittenQuery> rewriteQueriesLifeifei(
			ArrayList<FullQuery> allQueryList,
			ArrayList<Pair<Integer, Integer>> hittingQueryList,
			int rewrittenQueryID) {

		ArrayList<RewrittenQuery> resRewrittenQueryList = new ArrayList<RewrittenQuery>();
		resRewrittenQueryList.add(new RewrittenQuery());
		resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
				.setRewrittenQueryID(rewrittenQueryID);

		Pair<Integer, Integer> p1 = hittingQueryList.get(0);
		LocalQuery localQuery1 = allQueryList.get(p1.first).getLocalQuery(
				p1.second);
		resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).HittingQuerySet
				.addAll(hittingQueryList);

		if (hittingQueryList.size() == 1) {

			ArrayList<TriplePattern> curTriplePatternList = localQuery1
					.getTriplePatternList();
			TreeMap<String, String> tmpOriginalVarMap = new TreeMap<String, String>();
			TreeMap<String, String> tmpRenamedVarMap = new TreeMap<String, String>();
			int cur_var_id = 0;
			for (int i = 0; i < curTriplePatternList.size(); i++) {
				TriplePattern tp = curTriplePatternList.get(i);
				TriplePattern new_tp = new TriplePattern();

				cur_var_id = setTriplePatternInMainPattern(tp, new_tp,
						rewrittenQueryID, cur_var_id, tmpOriginalVarMap,
						tmpRenamedVarMap);

				resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).MainPatternGraph
						.addTriplePattern(new_tp);
			}

			resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).OriginalVarMapList
					.add(tmpOriginalVarMap);
			resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).RenamedVarMapList
					.add(tmpRenamedVarMap);
			resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).sourceList
					.addAll(localQuery1.getSourceList());
			resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).ConstraintList
					.add(new ArrayList<Pair<String, String>>());
			// resRewrittenQueryList.get(resRewrittenQueryList.size() -
			// 1).OptionalPatternList
			// .add(new BGPGraph());

		} else {
			Pair<Integer, Integer> p2 = hittingQueryList.get(1);
			LocalQuery localQuery2 = allQueryList.get(p2.first).getLocalQuery(
					p2.second);
			// LocalQuery commonQuery = new LocalQuery();
			int cur_var_id = 0;
			TreeMap<String, String> tmpOriginalVarMap = new TreeMap<String, String>();
			TreeMap<String, String> tmpRenamedVarMap = new TreeMap<String, String>();

			for (int j = 0; j < localQuery1.getTriplePatternList().size(); j++) {
				TriplePattern tp = localQuery1.getTriplePatternList().get(j);

				if (findTriplePatternInLocalQuery(tp,
						localQuery2.getTriplePatternList()) != -1) {
					TriplePattern new_tp = new TriplePattern();

					cur_var_id = setTriplePatternInMainPattern(tp, new_tp,
							rewrittenQueryID, cur_var_id, tmpOriginalVarMap,
							tmpRenamedVarMap);

					// commonQuery.addTriplePattern(new_tp);
					resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).MainPatternGraph
							.addTriplePattern(new_tp);
				}
			}

			for (int j = 0; j < hittingQueryList.size(); j++) {
				Pair<Integer, Integer> p = hittingQueryList.get(j);
				LocalQuery curLocalQuery = allQueryList.get(p.first)
						.getLocalQuery(p.second);
				cur_var_id = 0;

				TreeMap<String, String> tmpOptionalOriginalVarMap = new TreeMap<String, String>();
				TreeMap<String, String> tmpOptionalRenamedVarMap = new TreeMap<String, String>();
				resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).FilterExpressionList
						.add(new ArrayList<ArrayList<Pair<String, String>>>());
				ArrayList<Pair<String, String>> tmpConstraintList = new ArrayList<Pair<String, String>>();

				tmpOptionalOriginalVarMap.putAll(tmpOriginalVarMap);
				tmpOptionalRenamedVarMap.putAll(tmpRenamedVarMap);
				BGPGraph tmpOptionalPattern = new BGPGraph();

				for (int i = 0; i < curLocalQuery.getTriplePatternList().size(); i++) {

					TriplePattern tp = curLocalQuery.getTriplePatternList()
							.get(i);
					if (findTriplePatternInLocalQuery(
							tp,
							resRewrittenQueryList.get(resRewrittenQueryList
									.size() - 1).MainPatternGraph.triplePatternList) == -1) {
						ArrayList<Pair<String, String>> tmpFilterExpList = new ArrayList<Pair<String, String>>();
						TriplePattern new_tp = new TriplePattern();
						cur_var_id = setTriplePatternInOptionalPattern(tp,
								new_tp, rewrittenQueryID, j, cur_var_id,
								tmpOptionalOriginalVarMap,
								tmpOptionalRenamedVarMap, tmpFilterExpList);
						tmpOptionalPattern.addTriplePattern(new_tp);
						resRewrittenQueryList
								.get(resRewrittenQueryList.size() - 1).FilterExpressionList
								.get(resRewrittenQueryList
										.get(resRewrittenQueryList.size() - 1).FilterExpressionList
										.size() - 1).add(tmpFilterExpList);
						tmpConstraintList.addAll(tmpFilterExpList);
					}
				}
				resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).OptionalPatternList
						.add(tmpOptionalPattern);
				resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).OriginalVarMapList
						.add(tmpOptionalOriginalVarMap);
				resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).RenamedVarMapList
						.add(tmpOptionalRenamedVarMap);
				resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).ConstraintList
						.add(tmpConstraintList);

			}
			resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).sourceList
					.addAll(localQuery1.getSourceList());
		}

		return resRewrittenQueryList;
	}

	private static int findTriplePatternInLocalQuery(TriplePattern tp,
			ArrayList<TriplePattern> tpList) {
		for (int i = 0; i < tpList.size(); i++) {
			if (tp.getSignature().equals(tpList.get(i).getSignature())) {
				return i;
			}
		}

		return -1;
	}

	private static int setTriplePatternInOptionalPattern(TriplePattern tp,
			TriplePattern new_tp, int rewrittenQueryID, int opt_var_id,
			int pre_var_count, TreeMap<String, String> tmpOriginalVarMap,
			TreeMap<String, String> tmpRenamedVarMap,
			ArrayList<Pair<String, String>> tmpConstraintList) {

		int cur_var_count = pre_var_count;

		new_tp.setSubjectVarTag(tp.isSubjectVar());
		if (tp.isSubjectVar()) {
			if (!tmpOriginalVarMap.containsKey(tp.getSubjectStr())) {
				new_tp.setSubjectStr("?rv_" + rewrittenQueryID + "_"
						+ opt_var_id + "_" + cur_var_count);
				cur_var_count++;

				tmpOriginalVarMap.put(tp.getSubjectStr(),
						new_tp.getSubjectStr());
				tmpRenamedVarMap
						.put(new_tp.getSubjectStr(), tp.getSubjectStr());
			}

			new_tp.setSubjectStr(tmpOriginalVarMap.get(tp.getSubjectStr()));
		} else {

			new_tp.setSubjectStr("?rv_" + rewrittenQueryID + "_" + opt_var_id
					+ "_" + cur_var_count);
			cur_var_count++;

			tmpConstraintList.add(new Pair<String, String>(new_tp
					.getSubjectStr(), tp.getSubjectStr()));
		}

		new_tp.setPredicateVarTag(tp.isPredicateVar());
		if (tp.isPredicateVar()) {
			if (!tmpOriginalVarMap.containsKey(tp.getPredicateStr())) {
				new_tp.setPredicateStr("?rv_" + rewrittenQueryID + "_"
						+ opt_var_id + "_" + cur_var_count);
				cur_var_count++;

				tmpOriginalVarMap.put(tp.getPredicateStr(),
						new_tp.getPredicateStr());
				tmpRenamedVarMap.put(new_tp.getPredicateStr(),
						tp.getPredicateStr());
			}
			new_tp.setPredicateStr(tmpOriginalVarMap.get(tp.getPredicateStr()));
		} else {
			new_tp.setPredicateStr(tp.getPredicateStr());
		}

		new_tp.setObjectVarTag(tp.isObjectVar());
		if (tp.isObjectVar()) {
			if (!tmpOriginalVarMap.containsKey(tp.getObjectStr())) {
				new_tp.setObjectStr("?rv_" + rewrittenQueryID + "_"
						+ opt_var_id + "_" + cur_var_count);
				cur_var_count++;

				tmpOriginalVarMap.put(tp.getObjectStr(), new_tp.getObjectStr());
				tmpRenamedVarMap.put(new_tp.getObjectStr(), tp.getObjectStr());
			}
			new_tp.setObjectStr(tmpOriginalVarMap.get(tp.getObjectStr()));
		} else {
			new_tp.setObjectStr("?rv_" + rewrittenQueryID + "_" + opt_var_id
					+ "_" + cur_var_count);
			cur_var_count++;

			tmpConstraintList.add(new Pair<String, String>(new_tp
					.getObjectStr(), tp.getObjectStr()));
		}

		return cur_var_count;
	}

	private static int setTriplePatternInMainPattern(TriplePattern tp,
			TriplePattern new_tp, int rewrittenQueryID, int pre_var_id,
			TreeMap<String, String> tmpOriginalVarMap,
			TreeMap<String, String> tmpRenamedVarMap) {

		int cur_var_id = pre_var_id;

		new_tp.setSubjectVarTag(tp.isSubjectVar());
		if (tp.isSubjectVar()) {
			if (!tmpOriginalVarMap.containsKey(tp.getSubjectStr())) {
				new_tp.setSubjectStr("?rv_" + rewrittenQueryID + "_"
						+ cur_var_id);
				cur_var_id++;
				tmpOriginalVarMap.put(tp.getSubjectStr(),
						new_tp.getSubjectStr());
				tmpRenamedVarMap
						.put(new_tp.getSubjectStr(), tp.getSubjectStr());
			}
			new_tp.setSubjectStr(tmpOriginalVarMap.get(tp.getSubjectStr()));

		} else {
			new_tp.setSubjectStr(tp.getSubjectStr());
		}

		new_tp.setPredicateVarTag(tp.isPredicateVar());
		if (tp.isPredicateVar()) {
			if (!tmpOriginalVarMap.containsKey(tp.getPredicateStr())) {
				new_tp.setPredicateStr("?rv_" + rewrittenQueryID + "_"
						+ cur_var_id);
				cur_var_id++;
				tmpOriginalVarMap.put(tp.getPredicateStr(),
						new_tp.getPredicateStr());
				tmpRenamedVarMap.put(new_tp.getPredicateStr(),
						tp.getPredicateStr());
			}
			new_tp.setPredicateStr(tmpOriginalVarMap.get(tp.getPredicateStr()));

		} else {
			new_tp.setPredicateStr(tp.getPredicateStr());
		}

		new_tp.setObjectVarTag(tp.isObjectVar());
		if (tp.isObjectVar()) {
			if (!tmpOriginalVarMap.containsKey(tp.getObjectStr())) {
				new_tp.setObjectStr("?rv_" + rewrittenQueryID + "_"
						+ cur_var_id);
				cur_var_id++;
				tmpOriginalVarMap.put(tp.getObjectStr(), new_tp.getObjectStr());
				tmpRenamedVarMap.put(new_tp.getObjectStr(), tp.getObjectStr());
			}
			new_tp.setObjectStr(tmpOriginalVarMap.get(tp.getObjectStr()));

		} else {
			new_tp.setObjectStr(tp.getObjectStr());
		}

		return cur_var_id;
	}

	public static ArrayList<RewrittenQuery> rewriteQueriesOPTIONAL(
			ArrayList<FullQuery> allQueryList, HittingSet curHittingSet,
			int rewrittenQueryID) {

		ArrayList<RewrittenQuery> resRewrittenQueryList = new ArrayList<RewrittenQuery>();
		resRewrittenQueryList.add(new RewrittenQuery());
		resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
				.setRewrittenQueryID(rewrittenQueryID);

		// generate the main pattern of rewritten query
		LocalQuery mainPattern = new LocalQuery();
		TriplePattern rewrittenMainPattern = new TriplePattern(
				curHittingSet.getTriplePattern(0));
		int cur_var_id = 0;
		if (rewrittenMainPattern.isSubjectVar()) {
			rewrittenMainPattern.setSubjectStr("?rv_" + rewrittenQueryID + "_"
					+ cur_var_id);
			cur_var_id++;
		}

		if (rewrittenMainPattern.isPredicateVar()) {
			rewrittenMainPattern.setPredicateStr("?rv_" + rewrittenQueryID
					+ "_" + cur_var_id);
			cur_var_id++;
		}

		if (rewrittenMainPattern.isObjectVar()) {
			rewrittenMainPattern.setObjectStr("?rv_" + rewrittenQueryID + "_"
					+ cur_var_id);
			cur_var_id++;
		}

		mainPattern.addTriplePattern(rewrittenMainPattern);
		resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
				.setMainPattern(mainPattern.getTriplePatternList());

		ArrayList<Pair<Integer, Integer>> curQueryList = curHittingSet
				.getHittingQuerySet();
		ArrayList<Pair<Integer, Integer>> curLocalGroup = new ArrayList<Pair<Integer, Integer>>();
		int optionalPatternIdx = 0;

		for (int i = 0; i < curQueryList.size(); i++) {

			Pair<Integer, Integer> p1 = curQueryList.get(i);
			if (curLocalGroup.contains(p1))
				continue;

			// curRewrittenQuery.addQuery(p1);
			LocalQuery curLocalQuery1 = allQueryList.get(p1.first)
					.getLocalQuery(p1.second);

			Integer[] mainMapping = curLocalQuery1.checkSubgraph(mainPattern);

			// commonQuery is a query graph that is isomorphic to a graph G
			// where G is a combination of main pattern and a graph in an
			// optional expression
			LocalQuery commonQuery = new LocalQuery();
			commonQuery
					.constructCommonQuery(curLocalQuery1, mainPattern,
							mainMapping, rewrittenQueryID, optionalPatternIdx,
							resRewrittenQueryList.get(resRewrittenQueryList
									.size() - 1),curQueryList.size(),i);
			optionalPatternIdx++;

			resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
					.increaseFilterList();
			for (int j = i; j < curQueryList.size(); j++) {
				Pair<Integer, Integer> p2 = curQueryList.get(j);
				LocalQuery curLocalQuery2 = allQueryList.get(p2.first)
						.getLocalQuery(p2.second);

				// a mapping from curLocalQuery2 to commonQuery
				// the subscript i is the id of curLocalQuery2
				// the mapping value mappingState[i] is the id of commonQuery
				Integer[] mappingState = commonQuery
						.checkIsomorphic(curLocalQuery2);
				if (mappingState != null) {
					resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
							.addQuery(p2);
					curLocalGroup.add(p2);
					resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
							.addMapping(mappingState, curLocalQuery2,
									commonQuery);

					for (int k = 0; k < curLocalQuery2.getSourceList().size(); k++) {
						resRewrittenQueryList.get(
								resRewrittenQueryList.size() - 1).addSource(
								curLocalQuery2.getSourceList().get(k));
					}

					// since we can only use optional, at each time we can only
					// rewrite one query into the rewritten query
					break;
				}
			}

			// check the filter expression in the last rewritten query. If
			// the filter expression is empty, return true; otherwise,
			// return false.
			if (curLocalGroup.size() != curQueryList.size()
					&& resRewrittenQueryList.get(
							resRewrittenQueryList.size() - 1)
							.isLastFilterEmpty()) {
				resRewrittenQueryList.add(new RewrittenQuery());
				resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
						.setRewrittenQueryID(rewrittenQueryID + 1);
				resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
						.setMainPattern(mainPattern.getTriplePatternList());
			}
		}

		return resRewrittenQueryList;
	}

	public RewrittenQuery() {
		super();
		this.MainPatternGraph = new BGPGraph();
		this.OptionalPatternList = new ArrayList<BGPGraph>();
		this.FilterExpressionList = new ArrayList<ArrayList<ArrayList<Pair<String, String>>>>();
		this.HittingQuerySet = new ArrayList<Pair<Integer, Integer>>();
		this.sourceList = new ArrayList<Integer>();
		this.bindingNames = new ArrayList<String>();
		this.resultList = new ArrayList<String[]>();
		// this.resultListOfMainPattern = new ArrayList<Result>();
		this.OriginalVarMapList = new ArrayList<TreeMap<String, String>>();
		this.RenamedVarMapList = new ArrayList<TreeMap<String, String>>();
		this.ConstraintList = new ArrayList<ArrayList<Pair<String, String>>>();
		// this.resultListOfHittingQuery = new ArrayList<ArrayList<String[]>>();
	}
	
	@Override
	public String toString() {
		return "RewrittenQuery [RewrittenQueryID=" + RewrittenQueryID
				+ ", MainPatternGraph=" + MainPatternGraph
				+ ", OptionalPatternList=" + OptionalPatternList
				+ ", HittingQuerySet=" + HittingQuerySet + ", sourceList="
				+ sourceList + ", bindingNames=" + bindingNames
				+ ", FilterExpressionList=" + FilterExpressionList
				+ ", ConstraintList=" + ConstraintList
				+ ", OriginalVarMapList=" + OriginalVarMapList
				+ ", RenamedVarMapList=" + RenamedVarMapList + ", isRewriteFilter=" + isRewriteFilter 
				+ ", isRewriteValues=" + isRewriteValues +" isRewriteOptional ="+isRewriteOptional
				+ ", score=" + score + ", resultList.size()=" + resultList.size()+"]";
	}
	public int getScore() {
		return score;
	}

	public void setIsRewriteFilter(boolean isTrue) {
		isRewriteFilter = isTrue;
	}
	public boolean getIsRewriteFilter() {
		return isRewriteFilter;
	}
	public void setScore(int reScore) {
		score = reScore;
	}
	public int getRewrittenQueryID() {
		return RewrittenQueryID;
	}

	public void setRewrittenQueryID(int rewrittenQueryID) {
		RewrittenQueryID = rewrittenQueryID;
	}

	public void addHittingQuery(Pair<Integer, Integer> p) {
		HittingQuerySet.add(p);
	}

	public BGPGraph getMainPatternGraph() {
		return MainPatternGraph;
	}

	public void setMainPatternGraph(BGPGraph mainPatternGraph) {
		MainPatternGraph = mainPatternGraph;
	}

	public ArrayList<BGPGraph> getOptionalPatternList() {
		return OptionalPatternList;
	}

	public void setOptionalPatternList(ArrayList<BGPGraph> optionalPatternList) {
		OptionalPatternList = optionalPatternList;
	}

	public ArrayList<Pair<Integer, Integer>> getHittingQuerySet() {
		return HittingQuerySet;
	}

	public void setHittingQuerySet(
			ArrayList<Pair<Integer, Integer>> hittingQuerySet) {
		HittingQuerySet = hittingQuerySet;
	}

	public ArrayList<Integer> getSourceList() {
		return sourceList;
	}

	public void setSourceList(ArrayList<Integer> sourceList) {
		this.sourceList = sourceList;
	}

	public ArrayList<String[]> getResultList() {
		return resultList;
	}

	public void setResultList(ArrayList<String[]> resultList) {
		this.resultList = resultList;
	}

	public ArrayList<ArrayList<ArrayList<Pair<String, String>>>> getFilterExpressionList() {
		return FilterExpressionList;
	}

	public void setFilterExpressionList(
			ArrayList<ArrayList<ArrayList<Pair<String, String>>>> filterExpressionList) {
		FilterExpressionList = filterExpressionList;
	}

	public void mergeSourceList(ArrayList<Integer> otherList) {
		for (int i = 0; i < otherList.size(); i++) {
			int curSource = otherList.get(i);
			if (!this.sourceList.contains(curSource)) {
				this.sourceList.add(curSource);
			}
		}
	}
	
	/**
	 * 把 重写查询列表的内容用SPARQL语句表达出来
	 * @return  返回一个SPARQL 查询
	 */
	public List<String> toSPARQLString() {
		List<String> sparqlList = new ArrayList<String>();
		String sparqlStr = "select * where { ";

		for (int i = 0; i < this.MainPatternGraph.getTriplePatternList().size(); i++) {
			TriplePattern curMainTriple = this.MainPatternGraph.getTriplePatternList().get(i);
			sparqlStr += curMainTriple.toTriplePatternString()+ "\t. \n";
			
			// 把主模式的Object变量换一下
/*			for (int k = 0; k < this.getOptionalPatternList().size(); k++) {
					ArrayList<TriplePattern> curBGPTriplePatternList = this.getOptionalPatternList().get(k).getTriplePatternList();
					for (int j = 0; j < curBGPTriplePatternList.size(); j++) {
						TriplePattern curTripleStr = curBGPTriplePatternList.get(j);
						if(!curMainTriple.isSubjectVar() && !curTripleStr.isSubjectVar() && curTripleStr.getSubjectStr().equals(curMainTriple.getSubjectStr())){
//							if(curMainTriple.isObjectVar() && curTripleStr.isObjectVar()){
								sparqlStr = sparqlStr.replace(curMainTriple.getObjectStr(), curTripleStr.getObjectStr());
//							}
							break;
						}
						// 两个predicate都是常量且相等
						else if(!curMainTriple.isPredicateVar() && !curTripleStr.isPredicateVar()&& curTripleStr.getPredicateStr().equals(curMainTriple.getPredicateStr())){
//							if(curMainTriple.isObjectVar() && curTripleStr.isObjectVar()){
								sparqlStr = sparqlStr.replace(curMainTriple.getObjectStr(), curTripleStr.getObjectStr());
//							}
							break;
						}				
				}
			}*/
		}
		System.out.println("> MainPatternGraph  :"+sparqlStr);
		String mainSparql = sparqlStr;
//		String allVluesStr = "";
//		String mainVar = "";
/*		if (this.getOptionalPatternList().size() >= 1) {
			sparqlStr += "OPTIONAL { ";
			System.out.println("> OPTIONAL  :"+sparqlStr);
		}*/
		
		/**
		 *  重写OPTIONAL 和 Filter（values）
		 */
		int optCount = 0;
//		boolean isHaveMain = false;
		for (int i = 0; i < this.getOptionalPatternList().size(); i++) {
//			sparqlStr = mainSparql;
			// optional 里面的三元组
			ArrayList<TriplePattern> curBGPTriplePatternList = this.getOptionalPatternList().get(i).getTriplePatternList();
			if (curBGPTriplePatternList.size() != 0) {
				// 每个OPTIONAL 的union
				if (this.getOptionalPatternList().size() > 1) {
//					sparqlStr += " OPTIONAL { ";
					
			 	 	if(optCount == 0)
					{ 	
						sparqlStr += " { ";
						optCount = 1;
					}else if(optCount >= 1){
						sparqlStr += " UNION { ";
						optCount = 2;
					}  			
					 
/* 					if(optCount == 0)
					{ 	
						sparqlStr += " { ";
						optCount = 1;
					}else if(optCount >= 1){
						sparqlStr += " UNION { ";
						optCount = 2;
					}  */
				}
				int FilterCount = 0;
				if (this.getFilterExpressionList().get(i).size() != 0) {
					// 如果是一个Filter的话，直接作为条件查询。多个的话，再改写为Filter
					for(int f1 = 0;f1 <this.getFilterExpressionList().get(i).size(); f1 ++){
						FilterCount += this.getFilterExpressionList().get(i).get(f1).size();
					}
				}
				// 添加OPTIONAL语句
 				for (int j = 0; j < curBGPTriplePatternList.size(); j++) {
					TriplePattern curTripleStr = curBGPTriplePatternList.get(j);
//					sparqlStr += curTripleStr.toTriplePatternString() + " . \n";
					/*
					 *  判断是否 主循环里面已经包括了这个三元组，已经包含的 不重复加
					 */
					TriplePattern curMainTriple = null;
					boolean isEqual = false;
					for (int k = 0; k < this.MainPatternGraph.getTriplePatternList().size(); k++) {
						curMainTriple = this.MainPatternGraph.getTriplePatternList().get(k);
						// 两个subject都是常量且相等
						if(!curMainTriple.isSubjectVar() && !curTripleStr.isSubjectVar() && curTripleStr.getSubjectStr().equals(curMainTriple.getSubjectStr())){
							isEqual = true;
							break;
						}
						// 两个object都是常量且相等
						else if(!curMainTriple.isObjectVar() && !curTripleStr.isObjectVar()  && curTripleStr.getObjectStr().equals(curMainTriple.getObjectStr())){
							isEqual = true;
							break;
						}
						// 两个predicate都是常量且相等
						else if(!curMainTriple.isPredicateVar() && !curTripleStr.isPredicateVar()&& curTripleStr.getPredicateStr().equals(curMainTriple.getPredicateStr())){
							isEqual = true;
							break;
						}						
					}
					 
					if(!isEqual){
						sparqlStr += curTripleStr.toTriplePatternString()+ " . \n";
						// 如果是一个Filter的话，直接作为条件查询
/*						if(this.getFilterExpressionList().get(i).get(0).size() == -1 ){
//							 System.out.println("一个Filter的话，直接作为条件查询");
							 String oneFileter = curTripleStr.toTriplePatternString();
							 Pair<String, String> p = this.getFilterExpressionList().get(i).get(0).get(0);
							 // subject 和Filter 的一个相等    ,
							 if(curTripleStr.getSubjectStr().equals(p.first)){
								 oneFileter = oneFileter.replace(curTripleStr.getSubjectStr()+"\t", p.second+"\t");//后面加空格。避免r1  r11 重复替换
							 }
							 else if(curTripleStr.getPredicateStr().equals(p.first)){
								 oneFileter = oneFileter.replace(curTripleStr.getPredicateStr()+"\t", p.second+"\t");
							 }
							 else if(curTripleStr.getObjectStr().equals(p.first)){
								 oneFileter = oneFileter.replace("\t"+curTripleStr.getObjectStr(), "\t"+p.second);
							 }
							 
							 if(mainSparql.contains(p.first)){
								 sparqlStr = sparqlStr.replace(p.first+"\t", p.second+"\t");
							 }
							sparqlStr += oneFileter+ " . \n";
							 
							
						}else{
							sparqlStr += curTripleStr.toTriplePatternString()+ " . \n";
						}*/
					}
				} 
				// Filter 改写 start
 				
// 				String valuesStr = "";
// 				isHaveMain = false;
				if (this.getFilterExpressionList().get(i).get(0).size() >= 1) {
					// 如果是一个Filter的话，直接作为条件查询。多个的话，再改写为Filter
					sparqlStr += " FILTER ( ";
//					sparqlStr += " values ";
					int count = 0;
					for (int j = 0; j < this.getFilterExpressionList().get(i).size(); j++) {      
						ArrayList<Pair<String, String>> filterExpList = this.getFilterExpressionList().get(i).get(j);
						String filterStr = "";
						
						// 修改前缀
/*	
     					String prefixStr = "";
     					String prefix = filterExpList.get(0).second;
						String tail = "";
						String flag = "";
						if(prefix.contains("<")&& prefix.contains(">")){
							flag = (prefix.contains("#"))?"#":"/";
							prefixStr = prefix.substring(1,prefix.lastIndexOf(flag)+1);
							tail = prefix.substring(prefix.lastIndexOf(flag)+1,prefix.length()-1);
							if(j==0){
								sparqlStr = "PREFIX p"+filterExpList.get(0).first.substring(1)+": <"+ prefixStr+">"+"\t"+sparqlStr;
							}
							prefix = "p"+filterExpList.get(0).first.substring(1)+":"+tail;
						}*/
						
/* 						if(j==0){
							valuesStr += filterExpList.get(0).first+" { ";
						} */
 						
						for (int expIdx = 0; expIdx < filterExpList.size(); expIdx++) {
							Pair<String, String> p = filterExpList.get(expIdx);
  							if (expIdx == 0) {
								filterStr += p.first + " = " + p.second;
							} else {
								filterStr += " && " + p.first + " = "+ p.second;
							}  
/* 							filterStr += " "+p.second+" ";
							if(!isHaveMain && mainSparql.contains(filterExpList.get(expIdx).first)){
								isHaveMain = true;
								mainVar = filterExpList.get(expIdx).first;
							} */
						}
//						valuesStr += filterStr;
//						if(isHaveMain){allVluesStr += filterStr;}				
 	 					if (!filterStr.equals("")) {
							if (count == 0) {
								sparqlStr += " ( " + filterStr + " ) ";
								count++;
							} else {
								sparqlStr += " || ( " + filterStr + " ) ";
							}
						}  
					}
/* 					if (!valuesStr.endsWith("values { ")) {
						valuesStr += " } ";
					}	*/
 					 
 					if (!sparqlStr.endsWith("FILTER ( ")) {
						sparqlStr += " ) ";
					}  else {
						sparqlStr = sparqlStr.substring(0,sparqlStr.length() - 9);
					} 
					
				}// Filter 改写 end
// 				if(!isHaveMain){  
//					sparqlStr += valuesStr;
//				} 
 				
				// 每个OPTIONAL 的union
 				if (this.getOptionalPatternList().size() > 1) {
					sparqlStr += " } \n";
				}  
			} // if end
		 }// optional 循环end
		
		// 变量个数
		int varCount = 0;
		for(char c : sparqlStr.toCharArray()){
			if(c=='?'){
				varCount ++;
			}
		}
		
		String old_Spl = sparqlStr; 
		// Join优化的重写查询 - optional
		if(this.isRewriteOptional && varCount>1){
			String subVar = "";
 			if(sparqlStr.indexOf("?") == sparqlStr.lastIndexOf("?")){//只有一个变量时
 				this.getOptionalJoinList().clear();
 				this.isRewriteOptional = false;
			}else{
				List<String> OPtBindNames = new ArrayList<String>();
				int strIndex = sparqlStr.indexOf("{"); 
		 
				String sparqlOptStr = sparqlStr.substring(strIndex+1);
				old_Spl = "select * where { ";
				for(int opt =0;opt< this.getOptionalJoinList().size();opt++){
	//				sparqlStr += " OPTIONAL { ";
					ArrayList<String> resutltOptional = this.getOptionalJoinList().get(opt).second;
					String optVar = this.getOptionalJoinList().get(opt).first+"\t"; //要替换的 相同变量
					if(!OPtBindNames.contains(optVar)){
						String optVaradd = optVar.replace("?", "");
						OPtBindNames.add(optVaradd);
					}
					String optReStr = "";
					boolean isNew = false;
					for(int j=0;j < resutltOptional.size();j++){ // 结果列表
						
						if(j == 0 || isNew){
							optReStr= " { "+sparqlOptStr.replace(optVar, resutltOptional.get(j)+"\t")+" } ";
							isNew = false;
						}else{
							optReStr= " Union { "+sparqlOptStr.replace(optVar, resutltOptional.get(j)+"\t")+" } ";
						}
						
//						System.out.println("optReStr  "+optReStr);
						subVar = optReStr.substring(optReStr.indexOf("?"));
						subVar = subVar.split("\t")[0]; //剩下的变量	
//						System.out.println("subVar  "+subVar);
						if(j==0 && !OPtBindNames.contains(subVar)){
							String subVaradd = subVar.replace("?", "");
							OPtBindNames.add(subVaradd);
						}
						optReStr = optReStr.replace(subVar+"\t", subVar+"__"+j+"\t");
						old_Spl += optReStr;
//						System.out.println("optReStr  "+optReStr);
						
						if((j>0 && j%1000 == 0 )|| j == (resutltOptional.size()-1)){
							// 添加到查询列表
							if (old_Spl.contains("OPTIONAL")) {
								old_Spl += " } \n";
							}
							old_Spl += " } ";
							sparqlList.add(old_Spl);	
							old_Spl = "select * where { ";
							isNew = true;
						}
					}
				}
				this.addAllBindingNames(OPtBindNames);
		   }
		}
		// Join优化的重写查询 - Filter
		else if(this.isRewriteFilter && varCount>1){
//			if (this.getFilterExpressionList().size() != 0) {
			// 如果是一个Filter的话，直接作为条件查询。多个的话，再改写为Filter
				for(int ft=0;ft< this.getFilterExpressionList().size();ft++){
					old_Spl = sparqlStr + "FILTER ( ";
					int count = 0;
					boolean isNew = false;
					for (int j = 0; j < this.getFilterExpressionList().get(ft).size(); j++) { 
						ArrayList<Pair<String, String>> filterExpList = this.getFilterExpressionList().get(ft).get(j);
						String filterStr = "";
						String prefix = filterExpList.get(0).second;
					/*	String prefixStr = "";
						String tail = "";
						String flag = "";
						if(prefix.contains("<")&& prefix.contains(">")){
							flag = (prefix.contains("#"))?"#":"/";
							prefixStr = prefix.substring(1,prefix.lastIndexOf(flag)+1);
							tail = prefix.substring(prefix.lastIndexOf(flag)+1,prefix.length()-1);
							if(j==0){
								sparqlStr = "PREFIX p"+filterExpList.get(0).first.substring(1)+": <"+ prefixStr+">"+"\t"+sparqlStr;
							}
							prefix = "p"+filterExpList.get(0).first.substring(1)+":"+tail;
						}*/
						// Filter改写查询
						for (int expIdx = 0; expIdx < filterExpList.size(); expIdx++) {
							Pair<String, String> p = filterExpList.get(expIdx);
							if (expIdx == 0) {
								filterStr += p.first + " = " +prefix;
							} else {
								filterStr += " && " + p.first + " = "+prefix;
							}
						}
						if (!filterStr.equals("")) {
							if (count == 0 || isNew) {
								old_Spl += " ( " + filterStr + " ) ";
								isNew = false;
								count++;
							} else {
								old_Spl += " || ( " + filterStr + " ) ";
							}
						}
						
						if((j>0 && j%1000 == 0 )|| j == (this.getFilterExpressionList().get(ft).size()-1)){
							if (!old_Spl.endsWith("FILTER ( ")) {
								old_Spl += " ) ";
							} else {
								old_Spl = old_Spl.substring(0,old_Spl.length() - 9);
							}
							// 添加到查询列表
							if (old_Spl.contains("OPTIONAL")) {
								old_Spl += " } \n";
							}
							old_Spl += " } ";
							sparqlList.add(old_Spl);	
							old_Spl = sparqlStr + "FILTER ( ";
							isNew = true;
						}
					}

//					System.out.println("> FILTER :"+sparqlStr);
//				}
			}// Filter 改写 end			
		}
		// Join优化的重写查询 - Values
		else if(this.isRewriteValues && varCount>1){
//			String filterStr = "";
//			String prefixStr = "";
//			String tail = "";
//			String flag = "";
			
			for(int val=0;val< this.getValuesList().size();val++){
				
				if(this.getValuesList().get(val).second=="" || this.getValuesList().get(val).second.replace(" ", "").length()==0)continue;
				old_Spl = sparqlStr;
				// 加前缀
/*				String prefix = this.getValuesList().get(i).second.split("\t")[0];
				if(prefix.contains("<")&& prefix.contains(">")){
					flag = (prefix.contains("#"))?"#":"/";
					prefixStr = prefix.substring(1,prefix.lastIndexOf(flag)+1);
					tail = "p"+this.getValuesList().get(0).first.substring(1);
					if(i==0){
						sparqlStr = "PREFIX p"+this.getValuesList().get(0).first.substring(1)+": <"+ prefixStr+">"+"\t"+sparqlStr;
					}
					prefix = this.getValuesList().get(i).second.replace("<"+prefixStr, "p"+this.getValuesList().get(0).first.substring(1)+":").replace(">", "");
				}
				sparqlStr += " values \t"+this.getValuesList().get(i).first+" { "+ prefix +" } ";*/
				
				old_Spl += " values \t"+this.getValuesList().get(val).first+" { "+ this.getValuesList().get(val).second +" } ";
 				if (old_Spl.contains("OPTIONAL")) {
					old_Spl += " } \n";
				} 
				old_Spl += " } ";
				sparqlList.add(old_Spl);
			}
		}else{
			// 没有join优化的查询
   			if (sparqlStr.contains("OPTIONAL")) {
				sparqlStr += " } \n";
			} 		
/*			if(isHaveMain){
				allVluesStr = " values "+mainVar+" { "+allVluesStr+" } ";
				sparqlStr += allVluesStr;
			}*/
			sparqlStr += " } ";
			sparqlList.add(sparqlStr);
		}
	
		return sparqlList;
	}

	public void addResult(String[] r) {
		this.resultList.add(r);
	}

	public ArrayList<TreeMap<String, String>> getOriginalVarMapList() {
		return OriginalVarMapList;
	}

	public void setOriginalVarMapList(
			ArrayList<TreeMap<String, String>> originalVarMapList) {
		OriginalVarMapList = originalVarMapList;
	}

	public ArrayList<TreeMap<String, String>> getRenamedVarMapList() {
		return RenamedVarMapList;
	}

	public void setRenamedVarMapList(
			ArrayList<TreeMap<String, String>> renamedVarMapList) {
		RenamedVarMapList = renamedVarMapList;
	}

	public ArrayList<String> getBindingNames() {
		return bindingNames;
	}

	public void setBindingNames(ArrayList<String> bindingNames) {
		this.bindingNames = bindingNames;
	}

	public void addAllBindingNames(List<String> bindingNames) {
		for (int i = 0; i < bindingNames.size(); i++) {
			this.bindingNames.add("?" + bindingNames.get(i));
		}
	}
	
	/**
	 *  添加了同构的Filter语句
	 * @param mappingState  和主模式同构的 那个ID
	 * @param curLocalQuery2  
	 * @param commonQuery
	 */
	public void addMapping(Integer[] mappingState, LocalQuery curLocalQuery2,LocalQuery commonQuery) {
		TreeMap<String, String> tmpRenamedVarMap = new TreeMap<String, String>();
		TreeMap<String, String> tmpOriginalVarMap = new TreeMap<String, String>();
		ArrayList<Pair<String, String>> filterExpList = new ArrayList<Pair<String, String>>();

		for (int i = 0; i < mappingState.length; i++) {
			// 那个ID对应的值，可能是变量？x  可能是uri <http://>
			String curOriginalStr = curLocalQuery2.getLocalBGP().IDVertexmap.get(i);
			// 共同查询中对应的值。可能是变量？x  可能是uri <htt://>
			String curRenamedStr = commonQuery.getLocalBGP().IDVertexmap.get(mappingState[i]);
			
			if (curOriginalStr.startsWith("?") && curRenamedStr.startsWith("?")) {
				// 都是变量
				tmpOriginalVarMap.put(curOriginalStr, curRenamedStr);
				tmpRenamedVarMap.put(curRenamedStr, curOriginalStr);
			} else if (!curOriginalStr.startsWith("?") && curRenamedStr.startsWith("?")) {
				// 只有共同查询中的  是变量
				if (!this.MainPatternGraph.triplePatternList.toString().contains(curOriginalStr)) {
					// 主模式不包含 那个对应的原始查询uri
					filterExpList.add(new Pair<String, String>(curRenamedStr,curOriginalStr));

				}
			}
		}
		this.RenamedVarMapList.add(tmpRenamedVarMap);
		this.OriginalVarMapList.add(tmpOriginalVarMap);
		boolean isExitFilter = false;
//		for(int i=0;i<this.FilterExpressionList.size();i++){
		int i = this.FilterExpressionList.size() - 1;
			for(int j =0;j<this.FilterExpressionList.get(i).size();j++){
				for(int k =0;k<this.FilterExpressionList.get(i).get(j).size();k++){
					for(int f =0;f<filterExpList.size();f++){
						if(this.FilterExpressionList.get(i).get(j).get(k).first.equals(filterExpList.get(f).first)&&
								this.FilterExpressionList.get(i).get(j).get(k).second.equals(filterExpList.get(f).second)){
							isExitFilter = true;break;
						}	
					}
					if(isExitFilter)break;
				}
				if(isExitFilter)break;
			}
//		}
		if(!isExitFilter){
			this.FilterExpressionList.get(this.FilterExpressionList.size() - 1).add(filterExpList);
		}
		this.ConstraintList.add(filterExpList);
	}

	public void increaseFilterList() {
		this.FilterExpressionList.add(new ArrayList<ArrayList<Pair<String, String>>>());
	}

	public ArrayList<ArrayList<Pair<String, String>>> getConstraintList() {
		return ConstraintList;
	}

	public void setConstraintList(
			ArrayList<ArrayList<Pair<String, String>>> constraintList) {
		ConstraintList = constraintList;
	}

	public void addSource(int sourceID) {
		if (!this.sourceList.contains(sourceID)) {
			this.sourceList.add(sourceID);
		}
	}

	public boolean containsQuery(Pair<Integer, Integer> p1) {
		return this.HittingQuerySet.contains(p1);
	}

	public void addQuery(Pair<Integer, Integer> p1) {
		this.HittingQuerySet.add(p1);
	}

	public void setMainPattern(ArrayList<TriplePattern> triplePatternList) {
		this.MainPatternGraph.setTriplePatternList(triplePatternList);
		for (int i = 0; i < triplePatternList.size(); i++) {
			TriplePattern curTriplePattern = triplePatternList.get(i);
			if (curTriplePattern.isSubjectVar()
					&& !this.MainPatternGraph.VertexIDmap
							.containsKey(curTriplePattern.getSubjectStr())) {
				this.MainPatternGraph.IDVertexmap.put(
						this.MainPatternGraph.IDVertexmap.size(),
						curTriplePattern.getSubjectStr());
				this.MainPatternGraph.VertexIDmap.put(
						curTriplePattern.getSubjectStr(),
						this.MainPatternGraph.VertexIDmap.size());
			}

			if (curTriplePattern.isObjectVar()
					&& !this.MainPatternGraph.VertexIDmap.containsKey(curTriplePattern.getObjectStr())) {
				this.MainPatternGraph.IDVertexmap.put(
						this.MainPatternGraph.IDVertexmap.size(),
						curTriplePattern.getObjectStr());
				this.MainPatternGraph.VertexIDmap.put(
						curTriplePattern.getObjectStr(),
						this.MainPatternGraph.VertexIDmap.size());
			}
		}
	}

	/**
	 * 添加OPTIONAL三元组
	 * @param curTriplePattern
	 * @param subjectMapping
	 * @param objectMapping
	 */
	public void addTriplePatternInOptional(TriplePattern curTriplePattern,int subjectMapping, int objectMapping) {
		BGPGraph curOptionalBGP = this.OptionalPatternList.get(this.OptionalPatternList.size() - 1);
		curOptionalBGP.triplePatternList.add(curTriplePattern);

		if (!curOptionalBGP.IDVertexmap.containsKey(subjectMapping)) {
			curOptionalBGP.IDVertexmap.put(subjectMapping,
					curTriplePattern.getSubjectStr());
			curOptionalBGP.VertexIDmap.put(curTriplePattern.getSubjectStr(),
					subjectMapping);
		}

		if (!curOptionalBGP.IDVertexmap.containsKey(objectMapping)) {
			curOptionalBGP.IDVertexmap.put(objectMapping,
					curTriplePattern.getObjectStr());
			curOptionalBGP.VertexIDmap.put(curTriplePattern.getObjectStr(),
					objectMapping);
		}

		if (!curOptionalBGP.AdjacencyMatrix.containsKey(subjectMapping)) {
			curOptionalBGP.AdjacencyMatrix.put(subjectMapping,
					new TreeMap<Integer, String>());
		}
		curOptionalBGP.AdjacencyMatrix.get(subjectMapping).put(objectMapping,
				curTriplePattern.getPredicateStr());
	}

	public int findSameFullQuery(Pair<Integer, Integer> p1) {
		for (int i = 0; i < this.HittingQuerySet.size(); i++) {
			// Pair<Integer, Integer> p = this.HittingQuerySet.get(i);
			if (this.HittingQuerySet.get(i).first == p1.first) {
				return i;
			}
		}

		return -1;
	}

	public int JoinWithFullQuery(ArrayList<FullQuery> allQueryList) {
		int res_count = 0;
		for (int i = 0; i < this.HittingQuerySet.size(); i++) {
			Pair<Integer, Integer> p1 = this.HittingQuerySet.get(i);

			ArrayList<String[]> tmpResList = new ArrayList<String[]>();
			FullQuery curFullQuery = allQueryList.get(p1.first);
			TreeMap<String, Integer> curVarIDMap = curFullQuery.getVarIDMap();
			TreeMap<String, String> curRenamedMap = this.RenamedVarMapList
					.get(i);
			int[] mappingPosArr = new int[this.bindingNames.size()];
			for (int j = 0; j < this.bindingNames.size(); j++) {
				String varStr = curRenamedMap.get(this.bindingNames.get(j));
				mappingPosArr[j] = curVarIDMap.get(varStr);
			}

			for (int k = 0; k < this.resultList.size(); k++) {
				String[] tmpRes = new String[curVarIDMap.size()];
				Arrays.fill(tmpRes, "");
				for (int j = 0; j < this.bindingNames.size(); j++) {
					tmpRes[mappingPosArr[j]] = this.resultList.get(k)[j];
				}
				tmpResList.add(tmpRes);
			}

			if (curFullQuery.getResultList().size() > 0) {
				int joining_pos = 0;
				for (int k = 0; k < curFullQuery.getVarIDMap().size(); k++) {
					if (!curFullQuery.getResult(0)[k].equals("")
							&& !tmpResList.get(0)[k].equals("")) {
						joining_pos = k;
						break;
					}
				}
				curFullQuery.Join(tmpResList, joining_pos);
			}
			res_count += curFullQuery.getResultList().size();
		}

		return res_count;

	}

	public void printResultsInLog(String fileStr) throws FileNotFoundException {
		PrintStream out = new PrintStream(new File(fileStr));

		for (int i = 0; i < this.resultList.size(); i++) {
			for (int j = 0; j < this.bindingNames.size(); j++) {
				out.print(this.bindingNames.get(j) + "\t"
						+ this.resultList.get(i)[j] + "\t");
			}
			out.println();
		}

		out.flush();
		out.close();

	}

	public void distributeResultsInLocalQuery(ArrayList<FullQuery> allQueryList) {
		for (int i = 0; i < this.HittingQuerySet.size(); i++) {
			Pair<Integer, Integer> p1 = this.HittingQuerySet.get(i);

			FullQuery curFullQuery = allQueryList.get(p1.first);
			LocalQuery curLocalQuery = allQueryList.get(p1.first)
					.getLocalQuery(p1.second);

			TreeMap<String, Integer> curVarIDMap = curFullQuery.getVarIDMap();
			TreeMap<String, String> tmpRenamedVarMap = this.RenamedVarMapList
					.get(i);
			int[] mappingPosArr = new int[this.bindingNames.size()];
			String[] mappingConstraintPosArr = new String[this.bindingNames
					.size()];
			Arrays.fill(mappingPosArr, -1);
			Arrays.fill(mappingConstraintPosArr, "");

			TreeMap<String, String> tmpConstraintMap = new TreeMap<String, String>();

			for (int k = 0; k < this.ConstraintList.get(i).size(); k++) {
				tmpConstraintMap.put(this.ConstraintList.get(i).get(k).first,
						this.ConstraintList.get(i).get(k).second);
			}

			for (int j = 0; j < this.bindingNames.size(); j++) {
				String varStr = this.bindingNames.get(j);
				if (tmpRenamedVarMap.containsKey(varStr)) {
					mappingPosArr[j] = curVarIDMap.get(tmpRenamedVarMap
							.get(varStr));
				}
				if (tmpConstraintMap.containsKey(varStr)) {
					mappingConstraintPosArr[j] = tmpConstraintMap.get(varStr);
					if (mappingConstraintPosArr[j].startsWith("<")
							&& mappingConstraintPosArr[j].endsWith(">")) {
						mappingConstraintPosArr[j] = mappingConstraintPosArr[j]
								.substring(1,
										mappingConstraintPosArr[j].length() - 1);
					}
				}
			}

			for (int k = 0; k < this.resultList.size(); k++) {
				String[] tmpRes = new String[curVarIDMap.size()];
				Arrays.fill(tmpRes, "");
				int tag = 0;
				for (int j = 0; j < this.bindingNames.size(); j++) {
					if (mappingPosArr[j] != -1) {
						tmpRes[mappingPosArr[j]] = this.resultList.get(k)[j];
						if (tmpRes[mappingPosArr[j]].equals("")) {
							tag = 1;
							break;
						}
					} else if (!mappingConstraintPosArr[j].equals("")) {
						if (!mappingConstraintPosArr[j].equals(this.resultList
								.get(k)[j])) {
							tag = 1;
							break;
						}
					}
				}
				if (tag == 0) {
					curLocalQuery.addResult(tmpRes);
				}
			}
		}
	}

	public void removeLocalQuery(int pos) {
		this.HittingQuerySet.remove(pos);
		this.RenamedVarMapList.remove(pos);
		this.OriginalVarMapList.remove(pos);
	}

	public void distributeResultsInHittingQuery(
			ArrayList<FullQuery> allQueryList) {
		for (int i = 0; i < this.HittingQuerySet.size(); i++) {
			Pair<Integer, Integer> p1 = this.HittingQuerySet.get(i);

			FullQuery curFullQuery = allQueryList.get(p1.first);
			ArrayList<String[]> tmpResList = new ArrayList<String[]>();

			TreeMap<String, Integer> curVarIDMap = curFullQuery.getVarIDMap();
			TreeMap<String, String> tmpRenamedVarMap = this.RenamedVarMapList
					.get(i);
			int[] mappingPosArr = new int[this.bindingNames.size()];
			String[] mappingConstraintPosArr = new String[this.bindingNames
					.size()];
			Arrays.fill(mappingPosArr, -1);
			Arrays.fill(mappingConstraintPosArr, "");

			TreeMap<String, String> tmpConstraintMap = new TreeMap<String, String>();

			for (int k = 0; k < this.ConstraintList.get(i).size(); k++) {
				tmpConstraintMap.put(this.ConstraintList.get(i).get(k).first,
						this.ConstraintList.get(i).get(k).second);
			}

			for (int j = 0; j < this.bindingNames.size(); j++) {
				String varStr = this.bindingNames.get(j);
				if (tmpRenamedVarMap.containsKey(varStr)) {
					mappingPosArr[j] = curVarIDMap.get(tmpRenamedVarMap
							.get(varStr));
				}
				if (tmpConstraintMap.containsKey(varStr)) {
					mappingConstraintPosArr[j] = tmpConstraintMap.get(varStr);
					if (mappingConstraintPosArr[j].startsWith("<")
							&& mappingConstraintPosArr[j].endsWith(">")) {
						mappingConstraintPosArr[j] = mappingConstraintPosArr[j]
								.substring(1,
										mappingConstraintPosArr[j].length() - 1);
					}
				}
			}

			for (int k = 0; k < this.resultList.size(); k++) {
				String[] tmpRes = new String[curVarIDMap.size()];
				Arrays.fill(tmpRes, "");
				int tag = 0;
				for (int j = 0; j < this.bindingNames.size(); j++) {
					if (mappingPosArr[j] != -1) {
						tmpRes[mappingPosArr[j]] = this.resultList.get(k)[j];
						if (tmpRes[mappingPosArr[j]].equals("")) {
							tag = 1;
							break;
						}
					} else if (!mappingConstraintPosArr[j].equals("")) {
						if (!mappingConstraintPosArr[j].equals(this.resultList.get(k)[j])) {
							tag = 1;
							break;
						}
					}
				}
				if (tag == 0) {
					tmpResList.add(tmpRes);
				}
			}
			// this.resultListOfHittingQuery.add(tmpResList);
		}
	}
	
	/**
	 * @param allQueryList  查询日志中的全部查询
	 * @param curHittingSet  当前三元组和它击中的查询
	 *        大概这样 HittingSet [triplePatternList=[TriplePattern 
	 *                [subjectStr=<http://bio2rdf.org/chebi:1>,predicateStr=?rv_0, objectStr=?rv_1]],
	 *                HittingQuerySet=[Pair [first=0, second=0], Pair [first=1, second=0], Pair [first=2, second=0]]]
	 * @param rewrittenQueryID   重写查询列表rewrittenQueryList的大小
	 * @return 
	 */
	public static ArrayList<RewrittenQuery> rewriteQueries(ArrayList<FullQuery> allQueryList, HittingSet curHittingSet,int rewrittenQueryID) {

			ArrayList<RewrittenQuery> resRewrittenQueryList = new ArrayList<RewrittenQuery>();
			resRewrittenQueryList.add(new RewrittenQuery());
			
			RewrittenQuery curResList = resRewrittenQueryList.get(resRewrittenQueryList.size() - 1);
			curResList.setRewrittenQueryID(rewrittenQueryID);
			// generate the main pattern of rewritten query
			LocalQuery mainPattern = new LocalQuery();
			// 当前重写的主模式（三元组） 
			TriplePattern rewrittenMainPattern = new TriplePattern(curHittingSet.getTriplePattern(0));
			int cur_var_id = 0;
			
			// 把变量?rv_0  进一步重写成  ?rv_0_0  这样
			if (rewrittenMainPattern.isSubjectVar()) {
				rewrittenMainPattern.setSubjectStr("?rv_" + rewrittenQueryID + "_"+ cur_var_id);
				cur_var_id++;
			}
			if (rewrittenMainPattern.isPredicateVar()) {
				rewrittenMainPattern.setPredicateStr("?rv_" + rewrittenQueryID+ "_" + cur_var_id);
				cur_var_id++;
			}
			if (rewrittenMainPattern.isObjectVar()) {
				rewrittenMainPattern.setObjectStr("?rv_" + rewrittenQueryID + "_"+ cur_var_id);
				cur_var_id++;
			}
 
			System.out.println("MainPattern "+rewrittenMainPattern);
			mainPattern.addTriplePattern(rewrittenMainPattern);
			// 设置主模式
			curResList.setMainPattern(mainPattern.getTriplePatternList()); 
	
			ArrayList<Pair<Integer, Integer>> curQueryList = curHittingSet.getHittingQuerySet();
			ArrayList<Pair<Integer, Integer>> curLocalGroup = new ArrayList<Pair<Integer, Integer>>();
			int optionalPatternIdx = 0;
			boolean isMainTriple = false;
			int pairCount = 0;
			
			// 遍历每个击中的Pair 
            //[first=0, second=0], Pair [first=1, second=0], Pair [first=2, second=0]
			int[] same2Main = new int[curQueryList.size()];
			for (int i = 0; i < curQueryList.size(); i++) {
				// 判断击中的查询中哪些和主模式完全相同
				Pair<Integer, Integer> p1 = curQueryList.get(i);
				LocalQuery curLocalQuery1 = allQueryList.get(p1.first).getLocalQuery(p1.second);
				isMainTriple = false;
				if(curLocalQuery1.getTriplePatternList().size() == 1){
					TriplePattern curMainTriple = curLocalQuery1.getTriplePatternList().get(0);
					// 判断是否该语句就等于主模式
					if(!curMainTriple.isSubjectVar() && curMainTriple.getSubjectStr().equals(rewrittenMainPattern.getSubjectStr())){
						isMainTriple = true;
					}else if(!curMainTriple.isObjectVar() && curMainTriple.getObjectStr().equals(rewrittenMainPattern.getObjectStr())){
						isMainTriple = true;
					}else if(!curMainTriple.isPredicateVar()&& curMainTriple.getPredicateStr().equals(rewrittenMainPattern.getPredicateStr())){
						isMainTriple = true;
					}
					
					if(isMainTriple){
						// 保存和主模式一样的
						same2Main[i] = i;
						pairCount ++;
					}else{
						same2Main[i] = -1;
					}
				}else{
					same2Main[i] = -1;
				}
			}// for  end
			isMainTriple = false;
			
			for (int i = 0; i < curQueryList.size(); i++) {
				Pair<Integer, Integer> p1 = curQueryList.get(i);
				if (curLocalGroup.contains(p1) ){ //已包含 p1
//				 //在之前已经和别的查询同构的三元组子图
					continue;
				}
/*				if(isMainTriple && MainTripleId!= -1){
					 curResList = resRewrittenQueryList.get(MainTripleId);
				}*/
				int sameReNo = -1;
				if(isMainTriple){
					for(int re : same2Main){
						if(re == i){
							sameReNo = re;
							break;
						}
					}
				}
				if(sameReNo == i){
					continue;
				}
				
				// curRewrittenQuery.addQuery(p1);
				// curLocalQuery1:  和当前三元组匹配的 一个查询
				LocalQuery curLocalQuery1 = allQueryList.get(p1.first).getLocalQuery(p1.second);
				
				// 在这个查询中，匹配主模式的 三元组 的结构 位置 如[0,1],[0,3]
				Integer[] mainMapping = curLocalQuery1.checkSubgraph(mainPattern);
	
				LocalQuery commonQuery = new LocalQuery();				
/*				mainPattern = new LocalQuery();
				mainPattern.addTriplePattern(curResList.getMainPatternGraph().getTriplePatternList().get(0));*/
				// commonQuery is a query graph that is isomorphic to a graph G
				// where G is a combination of main pattern and a graph in an optional expression				
				// 添加OPTIONAL语句
				commonQuery.constructCommonQuery(curLocalQuery1, mainPattern,mainMapping, rewrittenQueryID, optionalPatternIdx,curResList,curQueryList.size(), i);
				optionalPatternIdx++;
	             
				// 添加一个Filter列表
				curResList.increaseFilterList(); 
				if(allQueryList.get(p1.first).getQueryPattern().getTriplePatternList().size() == 1){
					// 当前查询只有一个三元组，而且是击中的查询里面的，所以这个查询就等于主模式
					 for(int re :same2Main){
						 //和主模式相同
//						 System.out.println("和主模式相同");
						 if(re != -1){
							Pair<Integer, Integer> mainP1 = curQueryList.get(re);
							LocalQuery mainCurLocalQuery = allQueryList.get(mainP1.first).getLocalQuery(mainP1.second);							 
							Integer[] mappingState = {0,1};
							curResList.addMapping(mappingState, mainCurLocalQuery,commonQuery);
							 // 添加资源列表
							 for (int k = 0; k < mainCurLocalQuery.getSourceList().size(); k++) {
								 curResList.addSource(mainCurLocalQuery.getSourceList().get(k)); 
							 } 
						 }
					 }
					 // 添加  击中的查询id
					 for(int reSame :same2Main){
						 if(reSame != -1){
							 curResList.addQuery(curQueryList.get(reSame));
						 }
					 }
					 isMainTriple = true;					 
					if(pairCount < curQueryList.size()){
						resRewrittenQueryList.add(new RewrittenQuery());
						resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).setRewrittenQueryID(rewrittenQueryID + 1);
						resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).setMainPattern(mainPattern.getTriplePatternList());
//						System.out.println(">ADD "+resRewrittenQueryList);
						curResList = resRewrittenQueryList.get(resRewrittenQueryList.size() - 1);
					}
					continue;
				}			

				for (int j = i; j < curQueryList.size(); j++) { 
					// 主模式击中的某个查询 ,[1,0]
					Pair<Integer, Integer> p2 = curQueryList.get(j);
					LocalQuery curLocalQuery2 = allQueryList.get(p2.first).getLocalQuery(p2.second);
					/**
					 *  a mapping from curLocalQuery2 to commonQuery the subscript i is the id of curLocalQuery2
					 *  the mapping value mappingState[i] is the id of commonQuery
					 */
//					Integer[] mappingState = null;
					// 检查同构的子图，可以重写为 Filter  
					Integer[] mappingState = commonQuery.checkIsomorphic(curLocalQuery2);
					if (mappingState != null) { // 最后一次遍历的时候，这里不为空 
					/*  for(Integer ii=0;ii<mappingState.length;ii++){
							System.out.print(mappingState[ii]+" ");
						} */
						pairCount ++;
						curResList.addQuery(p2); //  添加进它的Hitset
						curLocalGroup.add(p2);
						
						// 这里添加了Filter 语句
						curResList.addMapping(mappingState, curLocalQuery2,commonQuery);
						for (int k = 0; k < curLocalQuery2.getSourceList().size(); k++) {
							curResList.addSource(curLocalQuery2.getSourceList().get(k)); 
						}
//						System.out.println(">m "+resRewrittenQueryList);
					}
				}
				// check the filter expression in the last rewritten query. If
				// the filter expression is empty, return true; otherwise, return false.
				// 当前三元组对应的pair个数和Filter的大小不同等，并且   Filter为空
				if (curLocalGroup.size() != curQueryList.size() && curResList.isLastFilterEmpty()) {
					resRewrittenQueryList.add(new RewrittenQuery());
					resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).setRewrittenQueryID(rewrittenQueryID + 1);
					resRewrittenQueryList.get(resRewrittenQueryList.size() - 1).setMainPattern(mainPattern.getTriplePatternList());
//					System.out.println(">f "+resRewrittenQueryList);
				}			
			}// 遍历每个Pair end
			
			// 把空的Filter删掉
/*			int FilterCount = 0;
			if (curResList.getFilterExpressionList().size() != 0) {
				// 如果是一个Filter的话，直接作为条件查询。多个的话，再改写为Filter
				for(int f0=0;f0<curResList.getFilterExpressionList().size();f0++){
					FilterCount = 0;
					for(int f1 = 0;f1 <curResList.getFilterExpressionList().get(f0).size(); f1 ++){
						FilterCount += curResList.getFilterExpressionList().get(f0).get(f1).size();
					}	 
					if(FilterCount == 0){
						curResList.getFilterExpressionList().remove(f0);
					}
				}
			}	*/
			
			// 初始 重写查询的score
			int score = 0;
			for(int re=0;re < resRewrittenQueryList.size();re ++){
				score = 0;
				RewrittenQuery currentRe = resRewrittenQueryList.get(re);
				for(int f =0;f<currentRe.getFilterExpressionList().size(); f++){
					for(int p=0;p<currentRe.getFilterExpressionList().get(f).size();p++){
						score += currentRe.getFilterExpressionList().get(f).get(p).size();
					}
				}
				for(int f =0;f<currentRe.getOptionalPatternList().size(); f++){
					// 有的OPTIONALlist 是空的
					if(currentRe.getOptionalPatternList().get(f).getTriplePatternList().size()>0){
						score += currentRe.getOptionalPatternList().size();
						break;
					}
				}	
				currentRe.setScore(score);
			}
			

			
			return resRewrittenQueryList;
	 }
	
	
	

	public static ArrayList<RewrittenQuery> rewriteQueriesFILTER(
			ArrayList<FullQuery> allQueryList, HittingSet curHittingSet,
			int rewrittenQueryID) {

		ArrayList<RewrittenQuery> resRewrittenQueryList = new ArrayList<RewrittenQuery>();
		resRewrittenQueryList.add(new RewrittenQuery());
		resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
				.setRewrittenQueryID(rewrittenQueryID);

		// generate the main pattern of rewritten query
		LocalQuery mainPattern = new LocalQuery();
		TriplePattern rewrittenMainPattern = new TriplePattern(
				curHittingSet.getTriplePattern(0));
		int cur_var_id = 0;
		if (rewrittenMainPattern.isSubjectVar()) {
			rewrittenMainPattern.setSubjectStr("?rv_" + rewrittenQueryID + "_"
					+ cur_var_id);
			cur_var_id++;
		}

		if (rewrittenMainPattern.isPredicateVar()) {
			rewrittenMainPattern.setPredicateStr("?rv_" + rewrittenQueryID
					+ "_" + cur_var_id);
			cur_var_id++;
		}

		if (rewrittenMainPattern.isObjectVar()) {
			rewrittenMainPattern.setObjectStr("?rv_" + rewrittenQueryID + "_"
					+ cur_var_id);
			cur_var_id++;
		}

		mainPattern.addTriplePattern(rewrittenMainPattern);
		resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
				.setMainPattern(mainPattern.getTriplePatternList());

		ArrayList<Pair<Integer, Integer>> curQueryList = curHittingSet
				.getHittingQuerySet();
		ArrayList<Pair<Integer, Integer>> curLocalGroup = new ArrayList<Pair<Integer, Integer>>();
		int optionalPatternIdx = 0;

		for (int i = 0; i < curQueryList.size(); i++) {

			Pair<Integer, Integer> p1 = curQueryList.get(i);
			if (curLocalGroup.contains(p1))
				continue;

			// curRewrittenQuery.addQuery(p1);
			LocalQuery curLocalQuery1 = allQueryList.get(p1.first)
					.getLocalQuery(p1.second);

			Integer[] mainMapping = curLocalQuery1.checkSubgraph(mainPattern);

			// commonQuery is a query graph that is isomorphic to a graph G
			// where G is a combination of main pattern and a graph in an
			// optional expression
			LocalQuery commonQuery = new LocalQuery();
			commonQuery.constructCommonQuery(curLocalQuery1, mainPattern,
							mainMapping, rewrittenQueryID, optionalPatternIdx,
							resRewrittenQueryList.get(resRewrittenQueryList
									.size() - 1),curQueryList.size(),i);
			optionalPatternIdx++;

			resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
					.increaseFilterList();
			for (int j = i; j < curQueryList.size(); j++) {
				Pair<Integer, Integer> p2 = curQueryList.get(j);
				LocalQuery curLocalQuery2 = allQueryList.get(p2.first)
						.getLocalQuery(p2.second);

				// a mapping from curLocalQuery2 to commonQuery
				// the subscript i is the id of curLocalQuery2
				// the mapping value mappingState[i] is the id of commonQuery
				Integer[] mappingState = commonQuery
						.checkIsomorphic(curLocalQuery2);
				if (mappingState != null) {
					resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
							.addQuery(p2);
					curLocalGroup.add(p2);
					resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
							.addMapping(mappingState, curLocalQuery2,
									commonQuery);

					for (int k = 0; k < curLocalQuery2.getSourceList().size(); k++) {
						resRewrittenQueryList.get(
								resRewrittenQueryList.size() - 1).addSource(
								curLocalQuery2.getSourceList().get(k));
					}
				}
			}

			// add a new local query.
			if (curLocalGroup.size() != curQueryList.size()) {
				resRewrittenQueryList.add(new RewrittenQuery());
				resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
						.setRewrittenQueryID(rewrittenQueryID + 1);
				resRewrittenQueryList.get(resRewrittenQueryList.size() - 1)
						.setMainPattern(mainPattern.getTriplePatternList());
			}
		}

		return resRewrittenQueryList;
	}
	
	/**
	 * 判断当前Filter是否为空
	 * @return
	 */
	private boolean isLastFilterEmpty() {
		int filter_count = 0;
		for (int j = 0; j < this.FilterExpressionList.get(this.FilterExpressionList.size() - 1).size(); j++) {
			filter_count += this.FilterExpressionList.get(this.FilterExpressionList.size() - 1).get(j).size();
		}
		return filter_count == 0;
	}

	public ArrayList<Pair<String, String>> getValuesList() {
		return ValuesList;
	}

	public void setValuesList(ArrayList<Pair<String, String>> valuesList) {
		ValuesList = valuesList;
	}

	public boolean isRewriteValues() {
		return isRewriteValues;
	}

	public void setRewriteValues(boolean isRewriteValues) {
		this.isRewriteValues = isRewriteValues;
	}

	public ArrayList<Pair<String, ArrayList<String>>> getOptionalJoinList() {
		return OptionalJoinList;
	}

	public void setOptionalJoinList(ArrayList<Pair<String, ArrayList<String>>> optionalJoinList) {
		OptionalJoinList = optionalJoinList;
	}

	public boolean isRewriteOptional() {
		return isRewriteOptional;
	}

	public void setRewriteOptional(boolean isRewriteOptional) {
		this.isRewriteOptional = isRewriteOptional;
	}
}
