package step1;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;


public class CPD {
	private int visitedPoint;
	private int level;
	private HashMap<Integer,Integer> cand;
	private HashSet<Integer> excl;
	private ArrayList<Integer> result;
	/**
	 * @param visitedPoint
	 * @param level
	 * @param cand
	 * @param excl
	 */
	public CPD(int visitedPoint, int level, HashMap<Integer,Integer> cand,
			HashSet<Integer> excl) {
		this.visitedPoint = visitedPoint;
		this.level = level;
		this.cand = cand;
		this.excl = excl;
	}
	public CPD(String spdstr){
		read(spdstr);
	}
	public CPD(){
		
	}
	public int getVisitedPoint() {
		return visitedPoint;
	}
	public void setVisitedPoint(int visitedPoint) {
		this.visitedPoint = visitedPoint;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	public HashMap<Integer,Integer> getCand() {
		return cand;
	}
	public void setCand(HashMap<Integer,Integer> cand) {
		this.cand = cand;
	}
	public HashSet<Integer> getExcl() {
		return excl;
	}
	public void setExcl(HashSet<Integer> excl) {
		this.excl = excl;
	}
	public ArrayList<Integer> getResult() {
		return result;
	}
	public void setResult(ArrayList<Integer> result) {
		this.result = result;
	}
	public String toString(ArrayList<Integer> result) {
		StringBuilder re = new StringBuilder();
		re.append(this.visitedPoint + "%" + level + "%");
		String key = "";
		key = this.cand.keySet().toString();
		re.append(cand.size());
		re.append(",");// cand size
		re.append(key.substring(1, key.length() - 1));// cand content
		re.append("%");
		key = this.excl.toString();
		re.append(this.excl.size());
		re.append(",");// notset size
		re.append(key.substring(1, key.length() - 1));// notset content
		if(level > 1){
			re.append("%");
			if(this.result!=null && this.result.size()>0){
				for(int i = 0;i<this.level-1;i++){
					re.append(this.result.get(i)+",");//result content
				}
			}else{
				for(int i = 0;i<this.level-1;i++){
					re.append(result.get(i)+",");//result content
				}
			}
		}
		return re.toString();
	}
	
	public void read(String s) {
		String[] elms = s.split("%");
		this.visitedPoint = Integer.parseInt(elms[0]);
		this.level = Integer.parseInt(elms[1]);
		String candStr = elms[2];
		String[] ens = candStr.split(",");
		int candsize = Integer.parseInt(ens[0]);
		this.cand = new HashMap<Integer, Integer>(candsize);
		for (int i = 1; i < ens.length; i++) {
			this.cand.put(Integer.parseInt(ens[i].trim()), 0);
		}
		String notStr = elms[3];
		String[] enls = notStr.split(",");
		int notsize = Integer.parseInt(enls[0]);
		this.excl = new HashSet<Integer>(notsize);
		for (int i = 1; i < enls.length; i++) {
			this.excl.add(Integer.parseInt(enls[i].trim()));
		}
		this.result = new ArrayList<Integer>(this.level+this.cand.size()/2);
		if(level>1){
			enls = elms[4].split(",");
			for(int i = 0;i<this.level-1;i++){
				this.result.add(Integer.parseInt(enls[i]));
			}
		}
	}
}
