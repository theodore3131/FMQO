package Common;

import java.util.ArrayList;
import java.util.TreeMap;
/**
 * 
 * @author gq
 * LocalQuery 是当前查询(一个三元组)，以及它的结果，它所在的服务器
 * 也就是  在一个查询中，属于同一个数据库资源的分在一个LocalQuery里
 */
public class LocalQuery {
	private BGPGraph LocalBGP;
	private ArrayList<Integer> sourceList;
	private ArrayList<String[]> resultList;
	private boolean isMultiConstant = false;

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

	public void addResult(String[] r) {
		this.resultList.add(r);
	}

	public String[] getResult(int idx) {
		return this.resultList.get(idx);
	}

	@Override
	public String toString() {
		return "LocalQuery [LocalBGP=" + LocalBGP + ", sourceList="+ sourceList + ", resultList=" + resultList + "]";
	}

	public String toSPARQLString() {
		String triplePatternsStr = "";

		for (int i = 0; i < this.LocalBGP.triplePatternList.size(); i++) {
			TriplePattern curPattern = this.LocalBGP.triplePatternList.get(i);
			String curTriplePatternStr = "";
			curTriplePatternStr += curPattern.getSubjectStr() + "\t";
			curTriplePatternStr += curPattern.getPredicateStr() + "\t";
			curTriplePatternStr += curPattern.getObjectStr() + " .";
			triplePatternsStr += curTriplePatternStr + "\t";
		}

		return "select * where { " + triplePatternsStr + "}";
	}

	public BGPGraph getLocalBGP() {
		return LocalBGP;
	}

	public void setLocalBGP(BGPGraph localBGP) {
		LocalBGP = localBGP;
	}

	public LocalQuery() {
		super();
		this.LocalBGP = new BGPGraph();
		this.sourceList = new ArrayList<Integer>();
		this.resultList = new ArrayList<String[]>();
	}

	/** 
	 * @param curLocalQuery1   当前主模式属于的那个查询
	 * @param mainPattern    主模式
	 * @param mainMapping    主映射
	 * @param rewritternQueryID  重写查询的ID
	 * @param optionalPatternIdx   ”重写查询“rewriteQueries 自定义的一个递增数
	 * @param curRewrittenQuery  resRewrittenQueryList中的最后一个，也就是最新的一个
	 */
	public void constructCommonQuery(LocalQuery curLocalQuery1,LocalQuery mainPattern, Integer[] mainMapping,
			int rewritternQueryID, int optionalPatternIdx,RewrittenQuery curRewrittenQuery,int hitNum,int cur) {

		// TriplePattern mainTriplePattern = mainPattern.LocalBGP.getTriplePattern(0);
		// this.LocalBGP.addTriplePattern(mainTriplePattern);
		curRewrittenQuery.getOptionalPatternList().add(new BGPGraph());

		if(curLocalQuery1.getTriplePatternList().size() == 1 || curLocalQuery1.getIsMultiConstant()){
			
			int subjectID = 0, objectID = 0, subjectMapping = 0, objectMapping = 0;
			int var_id = 0;
			TreeMap<Integer, Integer> tmpVarID2NewIDMap = new TreeMap<Integer, Integer>();
			for (int i = 0; i < curLocalQuery1.LocalBGP.triplePatternList.size(); i++) {
				// 遍历 当前查询的每个三元组
				//curTriplePattern  当前三元组
				TriplePattern curTriplePattern = new TriplePattern(curLocalQuery1.LocalBGP.getTriplePattern(i));
				int tag = 0;
				
				// 当前三元组的Subject对应的ID
				subjectID = curLocalQuery1.LocalBGP.VertexIDmap.get(curTriplePattern.getSubjectStr());
				
				// 判断当前三元组的subject 是否属于主模式
				subjectMapping = searchInMapping(mainMapping, subjectID);
   				if(cur==0&&!mainPattern.LocalBGP.getTriplePatternList().get(0).isSubjectVar() && !curTriplePattern.isSubjectVar()){
						tag++;
						if (!tmpVarID2NewIDMap.containsKey(subjectID)) {
							var_id = tmpVarID2NewIDMap.size()+ mainPattern.LocalBGP.VertexIDmap.size();
							tmpVarID2NewIDMap.put(subjectID, var_id);
						}
						subjectMapping = tmpVarID2NewIDMap.get(subjectID);
						curTriplePattern.setSubjectStr("?rv_" + rewritternQueryID + "_"+ optionalPatternIdx + "_" + subjectMapping);				
				 }else{ 
						curTriplePattern.setSubjectStr(mainPattern.getLocalBGP().IDVertexmap.get(subjectMapping));
				}
				
				// 判断当前三元组的object 是否属于主模式
				objectID = curLocalQuery1.LocalBGP.VertexIDmap.get(curTriplePattern.getObjectStr());
				objectMapping = searchInMapping(mainMapping, objectID);
	 			if(cur==0&&!mainPattern.LocalBGP.getTriplePatternList().get(0).isObjectVar() && !curTriplePattern.isObjectVar()){
						 tag++; 
						 if (!tmpVarID2NewIDMap.containsKey(objectID)) {
								var_id = tmpVarID2NewIDMap.size()+ mainPattern.LocalBGP.VertexIDmap.size();
								tmpVarID2NewIDMap.put(objectID, var_id);
						 }
						 objectMapping = tmpVarID2NewIDMap.get(objectID);
						 curTriplePattern.setObjectStr("?rv_" + rewritternQueryID + "_"+ optionalPatternIdx + "_" + objectMapping);
				 }else{ 
						curTriplePattern.setObjectStr(mainPattern.getLocalBGP().IDVertexmap.get(objectMapping));
				 }
			 
				this.LocalBGP.addTriplePattern(curTriplePattern);
				if (tag > 0) {
					if(cur==0){
						mainPattern = new LocalQuery();
						mainPattern.addTriplePattern(curTriplePattern);
						curRewrittenQuery.setMainPattern(mainPattern.getTriplePatternList());
					}
					
					curRewrittenQuery.addTriplePatternInOptional(curTriplePattern,subjectMapping, objectMapping);
				}
		   }
			// 只有一个三元组，所以直接设为主模式的三元祖
			this.LocalBGP = mainPattern.LocalBGP;
			return;
		}

		
		int subjectID = 0, objectID = 0, subjectMapping = 0, objectMapping = 0;
		int var_id = 0;
		TreeMap<Integer, Integer> tmpVarID2NewIDMap = new TreeMap<Integer, Integer>();
//		boolean isMain = false;
		for (int i = 0; i < curLocalQuery1.LocalBGP.triplePatternList.size(); i++) {
			// 遍历 当前查询的每个三元组
			//curTriplePattern  当前三元组
			TriplePattern curTriplePattern = new TriplePattern(curLocalQuery1.LocalBGP.getTriplePattern(i));
			int tag = 0;
			
			// 当前三元组的Subject对应的ID
			subjectID = curLocalQuery1.LocalBGP.VertexIDmap.get(curTriplePattern.getSubjectStr());
			
			// 判断当前三元组的subject 是否属于主模式
			subjectMapping = searchInMapping(mainMapping, subjectID);
			if (subjectMapping == -1) { //  !mainPattern.LocalBGP.getTriplePatternList().get(0).isSubjectVar()
//				System.out.println("这个subject "+subjectID+"不属于主模式");
				tag++;
				if (!tmpVarID2NewIDMap.containsKey(subjectID)) {
					var_id = tmpVarID2NewIDMap.size()+ mainPattern.LocalBGP.VertexIDmap.size();
					tmpVarID2NewIDMap.put(subjectID, var_id);
				}
				subjectMapping = tmpVarID2NewIDMap.get(subjectID);
				curTriplePattern.setSubjectStr("?rv_" + rewritternQueryID + "_"+ optionalPatternIdx + "_" + subjectMapping);
			} 
			else {
/*				if(cur==0&&!mainPattern.LocalBGP.getTriplePatternList().get(0).isSubjectVar() && !curTriplePattern.isSubjectVar()){
					isMain = true;
					tag++;
					if (!tmpVarID2NewIDMap.containsKey(subjectID)) {
						var_id = tmpVarID2NewIDMap.size()+ mainPattern.LocalBGP.VertexIDmap.size();
						tmpVarID2NewIDMap.put(subjectID, var_id);
					}
					subjectMapping = tmpVarID2NewIDMap.get(subjectID);
					curTriplePattern.setSubjectStr("?rv_" + rewritternQueryID + "_"+ optionalPatternIdx + "_" + subjectMapping);				
				}else{*/
					curTriplePattern.setSubjectStr(mainPattern.getLocalBGP().IDVertexmap.get(subjectMapping));
//				}
			}
			
			// 判断当前三元组的object 是否属于主模式
			objectID = curLocalQuery1.LocalBGP.VertexIDmap.get(curTriplePattern.getObjectStr());
			objectMapping = searchInMapping(mainMapping, objectID);
			if (objectMapping == -1) { //!mainPattern.LocalBGP.getTriplePatternList().get(0).isObjectVar()
				tag++;
				if (!tmpVarID2NewIDMap.containsKey(objectID)) {
					var_id = tmpVarID2NewIDMap.size()+ mainPattern.LocalBGP.VertexIDmap.size();
					tmpVarID2NewIDMap.put(objectID, var_id);
				}
				objectMapping = tmpVarID2NewIDMap.get(objectID);
				curTriplePattern.setObjectStr("?rv_" + rewritternQueryID + "_"+ optionalPatternIdx + "_" + objectMapping);
			}
			else {
/*				 if(hitNum==1 && cur==0&&!mainPattern.LocalBGP.getTriplePatternList().get(0).isObjectVar() && !curTriplePattern.isObjectVar()){
						tag++;isMain = true;
						if (!tmpVarID2NewIDMap.containsKey(objectID)) {
							var_id = tmpVarID2NewIDMap.size()+ mainPattern.LocalBGP.VertexIDmap.size();
							tmpVarID2NewIDMap.put(objectID, var_id);
						}
						objectMapping = tmpVarID2NewIDMap.get(objectID);
						curTriplePattern.setObjectStr("?rv_" + rewritternQueryID + "_"+ optionalPatternIdx + "_" + objectMapping);
				}else{*/
					curTriplePattern.setObjectStr(mainPattern.getLocalBGP().IDVertexmap.get(objectMapping));
//				}
			}
			this.LocalBGP.addTriplePattern(curTriplePattern);

			if (tag > 0) {
/* 				if(isMain && cur ==0 && hitNum==1 ){
					mainPattern = new LocalQuery();
					mainPattern.addTriplePattern(curTriplePattern);
					curRewrittenQuery.setMainPattern(mainPattern.getTriplePatternList());	
					isMain = false;
				} */
				curRewrittenQuery.addTriplePatternInOptional(curTriplePattern,subjectMapping, objectMapping);
			}
		}
	}

	private int searchInMapping(Integer[] mainMapping, int objectID) {
		for (int i = 0; i < mainMapping.length; i++) {
			if (mainMapping[i] == objectID)
				return i;
		}
		return -1;
	}

	public void addTriplePattern(TriplePattern rewrittenMainPattern) {
		this.LocalBGP.addTriplePattern(rewrittenMainPattern);
	}

	public ArrayList<TriplePattern> getTriplePatternList() {
		return this.LocalBGP.getTriplePatternList();
	}

	public Integer[] checkSubgraph(LocalQuery sub) {
		return this.LocalBGP.checkSubgraph(sub.LocalBGP);
	}

	public Integer[] checkIsomorphic(LocalQuery sub) {
		return this.LocalBGP.checkIsomorphic(sub.LocalBGP);
	}

	public void sort() {
		this.LocalBGP.sort();
	}

	public boolean getIsMultiConstant() {
		return isMultiConstant;
	}

	public void setIsMultiConstant(boolean isMultiConstant) {
		this.isMultiConstant = isMultiConstant;
	}
}
