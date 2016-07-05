package stepR;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;
import java.util.StringTokenizer;

import main.RunOver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import step1.CPD;

public class BottleneckDetect {
	public static HashMap<Integer, Integer> vertex = new HashMap<Integer, Integer>(
			3000);
	public static HashMap<Integer, HashSet<Integer>> verEdge = new HashMap<Integer, HashSet<Integer>>(
			3000);
	// public static HashMap<Integer, Integer> mark = new HashMap<Integer,
	// Integer>();
	public static Stack<CPD> stack = new Stack<CPD>();
	public static ArrayList<Integer> result = new ArrayList<Integer>(30);
	public static long number = 0;
	public static int levelNumber = 0;
	public static int totalPart = 36;
	public static int tmpKey = 0;
	public static int count = (int)(Math.random()*23);
	public static boolean done = false;
	// public static ArrayList<Integer> thenode = new ArrayList<Integer>();//
	// 需要计算的节点集
	public static HashSet<Integer> thenode = new HashSet<Integer>();// 需要计算的节点集
	public static int triangleNumber = 0;
	public static int cutNumber = 0; // 较大的计算单元，切割次数
	public static int cutNumberAfter = 0;
	public static int Strategy = 0;
	public static long TimeThreshold = 0;
	public static int sizeN = 0;
	public static HashSet<Integer> hs = new HashSet<Integer>();
	public static boolean balanceOrNot = false;
	public static long tPhase = 0;
	public static int NodeSN = 0;
	public static File dirRoot = new File("/home/"+RunOver.usr+"/cliquehadoop/");
	public static File serial = new File(dirRoot, "serialNumber.txt");
	public static RandomAccessFile raf = null;
	public static int which = 0;// 区分reduce的唯一编号，用于写数据文件
	public static int MaxOne = 3;// 最小是三角形，初始化时即初始化为三角形
	public static int randomSelect = 0;
	public static HashSet<Integer> parts = new HashSet<Integer>();
	public static int numReducer = 0;

	public static class BottleneckDetectMapper extends
			Mapper<LongWritable, Text, PairTypeInt, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			int k = -1;

			String[] kvoriginal = str.split("\\t");
			k = Integer.parseInt(kvoriginal[0]);
			if (k == 0)
				return;
			StringTokenizer stk = new StringTokenizer(kvoriginal[1]);
			if (k == -1)// 子图信息
			{
				int type = Integer.valueOf(stk.nextToken("@"));
				int p = Integer.valueOf(stk.nextToken("@"));
				int tk = Integer.valueOf(stk.nextToken("@"));
				context.write(new PairTypeInt(type, p, tk), new Text(stk
						.nextToken("toend").substring(1)));
			} else// 边连接信息
			{
				try{
				int type = Integer.valueOf(stk.nextToken("#"));
				String[] parts = stk.nextToken("#").split(",");
				int tk = Integer.valueOf(stk.nextToken("#"));
				String toend = stk.nextToken("toend").substring(1);

				for (String i : parts)
					context.write(new PairTypeInt(1,
							Integer.parseInt(i.trim()), tk), new Text(toend));
				}catch(NoSuchElementException e){
					System.out.println(str);
					System.out.println(k+"--------------");
					System.out.println(kvoriginal[1]);
					throw e;
				}
			}

		}
	}

	public static class BKPBReducer extends
			Reducer<PairTypeInt, Text, IntWritable, Text> {
static int treesize = 0;
		HashSet<Integer> notset = new HashSet<Integer>();
		HashMap<Integer, Integer> cand = new HashMap<Integer, Integer>();
		//boolean reducefinished = false;
		long allStart = 0;
		long allEnd = 0;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			numReducer = context.getNumReduceTasks();
			FileReader fr = new FileReader(new File(dirRoot,
					"trianglenew_NODE.txt"));
			BufferedReader bfr = new BufferedReader(fr);
			// 提取出准备要处理的节点列表
			String record = "";
			while ((record = bfr.readLine()) != null) {
				String[] adjInfos = record.split(" ");
				for (int i = 0; i < adjInfos.length; i++)
					thenode.add(Integer.valueOf(adjInfos[i]));
			}
			bfr.close();

			FileReader fr2 = new FileReader(new File(dirRoot,
					"trianglenew_CLIQUE.txt"));
			BufferedReader bfr2 = new BufferedReader(fr2);
			// 参数配置信息
			String record2 = "";
			if ((record2 = bfr2.readLine()) != null) {
				String[] adjInfos = record2.split(" ");
				totalPart = Integer.valueOf(adjInfos[0]);
				Strategy = Integer.valueOf(adjInfos[1]);
				TimeThreshold = Long.valueOf(adjInfos[2]);
				sizeN = Integer.valueOf(adjInfos[3]);
				randomSelect = Integer.valueOf(adjInfos[4]);
			}
			bfr2.close();

			// 确定该reduce的唯一编号
			RandomAccessFile rafcur = null;
			try {
				rafcur = new RandomAccessFile(serial, "rw");
				FileChannel fc = rafcur.getChannel();
				;
				FileLock fl = null;
				boolean got = false;// 获得编号
				while (true) {
					Thread.sleep(20);
					try {
						// fc = rafcur.getChannel();
						fl = fc.tryLock();
						if (fl != null)// 可以锁住
						{
							if (serial.length() == 0)// 空文件
							{
								rafcur.write("0\n".getBytes());
								which = 0;
							} else {
								String line = rafcur.readLine();
								which = (Integer.valueOf(line) + 1);
								rafcur.seek(0);
								rafcur.write((which + "\n").getBytes());
							}
							got = true;
						} else {
							Thread.sleep(20);// 避免try过多
						}
					} finally {
						// if(rafcur!=null)
						// rafcur.close();
						if (fl != null) {
							try {
								fl.release();
							} catch (Exception e) {
							}
						}
					}
					if (got)// 获得了唯一编号
						break;
				}
			} finally {
				if (rafcur != null)
					rafcur.close();
			}
			// 获得唯一编号后打开文件
			File curReduce = new File(dirRoot, "/outresult/bkpb/"+which + "");
			raf = new RandomAccessFile(curReduce, "rw");
			System.out.println("this reducer is number:"+which+" and create file:"+curReduce.getPath());

			allStart = System.currentTimeMillis();
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			System.out.println("in clean up tphase="+tPhase+" timethreshold="+TimeThreshold);

			if(raf.getFilePointer()==0){
				raf.close();
				File f = new File(dirRoot, "/outresult/bkpb/"+which + "");
				boolean RES = f.delete();
				System.out.println("this reducer is number:"+which+" " +
						"and raf is empty so delete file:"+f.getPath()+" --"+RES);
			}else if (tPhase < TimeThreshold) {// 文件中有内容
				Stack<CPD> stack = new Stack<CPD>();
				verEdge.clear();
				stack.clear();
				result = null;
				raf.seek(0);
				// 重新写入这个文件，原来的文件作废，最后将其删除！
				RandomAccessFile rnew = new RandomAccessFile(new File(dirRoot, "/outresult/bkpb/"+which + "#"), "rw");
				System.out.println("this reducer is number:"+which+" " +
						"and in cleanup so create file:"+"/outresult/bkpb/"+which + "#");
				String line = "";
				long t1 = System.currentTimeMillis();
				long t2 = System.currentTimeMillis();
				while ((line = raf.readLine()) != null) {
					String[] ab = line.split("\t");
					if (Integer.parseInt(ab[0]) == -2) {
						// 是边邻接信息
						String b = ab[1];
						String[] vas = b.split("#");
						tmpKey = Integer.parseInt(vas[2]);
						String edgestring = vas[3];
						readVerEdge(edgestring, verEdge);
					} else {
						// 是子图信息
						String[] elements = ab[1].split("@");
						tmpKey = Integer.parseInt(elements[2]);
						CPD st = new CPD(elements[3]);
						stack.add(st);
						if (elements[0].equals("0")) {
							// 完整的子图包括verEdge
							readVerEdge(elements[4], verEdge);
						} else {
							// 仅子图信息
							continue;// 还没有读全，继续往下读
						}
					}
					//一个完整的子图已经读进来了，下面计算
					parts.clear();
					balanceOrNot = false;
					while(!stack.empty()){
						CPD top = stack.pop();
						HashSet<Integer> notset = top.getExcl();
						HashMap<Integer,Integer> cand = top.getCand();
						int level = top.getLevel();
						int vp = top.getVisitedPoint();
						if(top.getResult()!=null){
							result = top.getResult();
						}
						if(level+cand.size()<=MaxOne){
							continue;
						}
/*						if (allContained(cand, notset)) {
							continue;
						}
*/						if (result.size() + 1 == level) {
							result.add(vp);
						} else {
							result.set(level - 1, vp);
						}
						if (cand.isEmpty()) {
							if (notset.isEmpty()) {
								emitClique(result, level, cand, context);
							}
							continue;
						}
						int fixp = findMaxDegreePoint(cand);
treesize++;
						ArrayList<Integer> noneFixp = new ArrayList<Integer>(
								cand.size() - maxdeg);
						HashMap<Integer, Integer> tmpcand = genInterSet(cand,
								fixp, maxdeg, noneFixp);
						HashSet<Integer> tmpnot = genInterSet(notset, fixp);
						CPD tmp = new CPD(fixp, level + 1, tmpcand, tmpnot);
						if (tmpcand.size() <= sizeN) {
							enumerateClique(tmp,context);
							//stack.add(tmp);
						} else {
							stack.add(tmp);
						}
						notset.add(fixp);
						for (int fix : noneFixp) {
treesize++;
							HashMap<Integer, Integer> tcd = genInterSet(cand,
									fix);
							HashSet<Integer> tnt = genInterSet(notset, fix);
							CPD temp = new CPD(fix, level + 1, tcd, tnt);
							if(tPhase < TimeThreshold){
							if (tcd.size() <= sizeN) {
								enumerateClique(temp,context);
								///stack.add(temp);
							} else {
								stack.add(temp);
							}
						}else{
							spillToDisk(temp,rnew);
						}
//						if (tcd.size() <= sizeN && tPhase < TimeThreshold) {
//							enumerateClique(temp,context);
//							///stack.add(temp);
//						} else {
//							spillToDisk(temp,rnew);
//						}
							notset.add(fix);
							if(tPhase < TimeThreshold){
								t2 = System.currentTimeMillis();
								tPhase += (t2 - t1);
								t1 = t2;
							}
						}
						if (tPhase >= TimeThreshold) {
							break;
						}
					}
					
					while (!stack.empty()) {
						// 退出了栈还没空，说明时间到了还没计算完，把栈中的子图spill到磁盘
						spillToDisk(stack.pop(), rnew);
					}
					if (balanceOrNot) {
						rnew.write(((-2) + "\t" + 1 + "#" ).getBytes());
						String pas = parts.toString();
						rnew.write(pas.substring(1, pas.length()-1).getBytes());
						rnew.write(("#"+tmpKey+"#").getBytes());
						writeVerEdge(verEdge, rnew);
						rnew.write(("\n").getBytes());// rnew.write(("\n0\t0\n").getBytes());
					}

					if (tPhase >= TimeThreshold) {
						break;
					}
				}
				
				while ((line = raf.readLine()) != null) {
					// 把原文件后面的内容直接考到新文件后面
					rnew.write(line.getBytes());
					rnew.write("\n".getBytes());
				}
				raf.close();
				File endf = new File(dirRoot, "/outresult/bkpb/"+which + "");
				boolean endRES = endf.delete();
				System.out.println("this reducer is number:"+which+" " +
						"and clean up to end, so delete file:"+endf.getPath()+" --"+endRES);
				if(rnew.getFilePointer()==0){
					File tf = new File(dirRoot, "/outresult/bkpb/"+which + "#");
					boolean ntr = tf.delete();
					System.out.println("this reducer is number:"+which+" " +
							"and clean up to end & rnew is empty, so delete file:"+tf.getPath()+" --"+ntr);
				}else{
					rnew.close();
				}
			}else{
				System.out.println("time is already out before cleanup, so close raf and exit");
				raf.close();
			}
			
			super.cleanup(context);
			allEnd = System.currentTimeMillis();
			System.out.println("all time: "+(allEnd - allStart)/1000);
System.out.println("treesize: " + treesize);			
		}
		private int maxdeg;
		long t1;
		protected void reduce(PairTypeInt key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			t1 = System.currentTimeMillis();
			
			tmpKey = key.getC();
			int type = key.getA();
			vertex.clear();
			verEdge.clear();
			count = 0;
			notset.clear();
			cand.clear();
			parts.clear();
			number = 0;
			cutNumber = 0;
			cutNumberAfter = 0;
			balanceOrNot = false;
			if (tPhase < TimeThreshold) {
				for (Text t : values) {
					// 读入一个完整的子图集合
					String ver = t.toString();
					if (type == 1) {// 是没有邻接表的不完整子图，或者是邻接表
						if (ver.contains("%")) {
							// 是不全的子图
							stack.add(new CPD(ver));
						} else if(verEdge.size()==0){
							// 是邻接表
							readVerEdge(ver, verEdge);
						}//else{
							//verEdge == 0 时不用处理，因为同一个key的不同CPD对应同一个verEdge
					    //}
					} else {// 是一个完整的包括了邻接表的，没有切分过的size-1子图
						String[] sts = ver.split("@");
						stack.add(new CPD(sts[0]));
						readVerEdge(sts[1], verEdge);
					}
				}
			} else {
				// 边邻接信息写在最后
				String edgestr = "";
				parts.clear();
				for (Text t : values) {
					//ai...........
					//System.out.println("out" + t.toString());
					NodeSN++;
					String ver = t.toString();
					if (type == 1) {
						if (ver.contains("%")) {
							int p = NodeSN %numReducer;
							parts.add(p);
							raf.write(("-1\t1@" + p + "@"
									+ tmpKey + "@").getBytes());
							raf.write(ver.getBytes());
							raf.write("\n".getBytes());
						} else if(edgestr.length()==0){
							edgestr = ver;
						}
					} else {
						raf.write(("-1\t0@" + NodeSN % numReducer + "@" + tmpKey + "@")
								.getBytes());
						raf.write(ver.getBytes());
						raf.write("\n".getBytes());
					}
				}
				if(parts.size()>0){//有需要这条边信息的子图才发送.另外如果之前这个点只有未切割过的包含边的子图,那么parts和edgestr必然都为空
					String pstr = parts.toString();
					raf.write(((-2) + "\t" + 1 + "#" +pstr.substring(1, pstr.length()-1)).getBytes());
					
					raf.write(("#" + tmpKey + "#").getBytes());
					raf.write(edgestr.getBytes());
					raf.write("\n".getBytes());
				}
				
				return;
			}
			long t2 = System.currentTimeMillis();
			while (!stack.empty()) {
				if (tPhase < TimeThreshold) // 时间小于阈值
				{
					CPD top = stack.pop();
					HashSet<Integer> notset = top.getExcl();
					HashMap<Integer, Integer> cand = top.getCand();
					int level = top.getLevel();
					int vp = top.getVisitedPoint();
					//处理同一个key的不同part的情况,同一个key的不同part对应不同的result
					if (top.getResult() != null) {
						result = top.getResult();
					}
					if (level + cand.size() <= MaxOne) {
						continue;
					}
/*					if (allContained(cand, notset)) {
						continue;
					}
*/					if (result.size() + 1 == level) {
						result.add(vp);
					} else {
						result.set(level - 1, vp);
					}
					if (cand.isEmpty()) {
						if (notset.isEmpty()) {
							emitClique(result, level, cand, context);
						}
						continue;
					}
					int fixp = findMaxDegreePoint(cand);
					ArrayList<Integer> noneFixp = new ArrayList<Integer>(
							cand.size() - maxdeg);
					HashMap<Integer, Integer> tmpcand = genInterSet(cand, fixp,
							maxdeg, noneFixp);
					HashSet<Integer> tmpnot = genInterSet(notset, fixp);
					CPD tmp = new CPD(fixp, level + 1, tmpcand, tmpnot);
					if (tmpcand.size() <= sizeN) {
						enumerateClique(tmp, context);
						//stack.add(tmp);
					} else {
						//spillToDisk(tmp,raf);
						stack.add(tmp);
					}
					notset.add(fixp);
treesize++;
					for (int fix : noneFixp) {
						HashMap<Integer, Integer> tcd = genInterSet(cand, fix);
						HashSet<Integer> tnt = genInterSet(notset, fix);
						CPD temp = new CPD(fix, level + 1, tcd, tnt);
						if (tcd.size() <= sizeN && tPhase < TimeThreshold) {
							enumerateClique(temp, context);
							//stack.add(temp);
						} else {
							spillToDisk(temp,raf);
							//stack.add(temp);
						}
						notset.add(fix);
treesize++;
					}

					if(tPhase < TimeThreshold){
						t2 = System.currentTimeMillis();
						tPhase += (t2 - t1);
						t1 = t2;
					}
				} else {
					break;
				}
			}
			while (!stack.empty()) {// 未算完的spill到磁盘
				spillToDisk(stack.pop(), raf);
			}
			if(balanceOrNot){
				raf.write(((-2) + "\t" + 1 + "#" ).getBytes());
				String pas = parts.toString();
				raf.write(pas.substring(1, pas.length()-1).getBytes());
				raf.write(("#"+tmpKey+"#").getBytes());
				writeVerEdge(verEdge, raf);
				raf.write(("\n").getBytes());
			}
		}
		private void writeVerEdge(HashMap<Integer, HashSet<Integer>> edge,
				RandomAccessFile rf) throws IOException {
			for (Map.Entry<Integer, HashSet<Integer>> en : edge.entrySet()) {
				// rf.write(en.getKey());
				rf.write((en.getKey() + "=").getBytes());
				Iterator<Integer> it = en.getValue().iterator();
				rf.write(it.next().toString().getBytes());
				while (it.hasNext()) {
					rf.write(("," + it.next()).getBytes());
				}
				rf.write(" ".getBytes());
			}
		}
		private void spillToDisk(CPD ss, RandomAccessFile raf)
				throws IOException {
			balanceOrNot = true;
			StringBuilder sb = new StringBuilder();
			int p = count % numReducer;
			parts.add(p);
			sb.append("-1\t1@");
			sb.append(p);
			sb.append("@" + tmpKey + "@");
			sb.append(ss.toString(result));
			sb.append("\n");
			count++;
			raf.write(sb.toString().getBytes());
		}

		private HashMap<Integer, Integer> genInterSet(
				HashMap<Integer, Integer> cand, int aim) {
			HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();
			int acc = 0;
			HashSet<Integer> adj = verEdge.get(aim);
			cand.remove(aim);
			Set<Integer> small, big;
			if (adj.size() > cand.size()) {
				small = cand.keySet();
				big = adj;
			} else {
				big = cand.keySet();
				small = adj;
			}
			Iterator<Integer> it = small.iterator();
			int tmp;
			while (it.hasNext()) {
				tmp = it.next();
				if (big.contains(tmp)) {
					acc++;
					result.put(tmp, 0);
				}
			}
			return result;
		}

		/**
		 * compute a small subgraph to finish
		 * 
		 * @param tmp
		 * @param context
		 * @throws InterruptedException
		 * @throws IOException
		 */
		private void enumerateClique(CPD tmp, Context context)
				throws IOException, InterruptedException {
			Stack<CPD> stack = new Stack<CPD>();
			stack.add(tmp);
			while (!stack.empty()) {
				CPD top = stack.pop();
				HashSet<Integer> notset = top.getExcl();
				HashMap<Integer, Integer> cand = top.getCand();
				int level = top.getLevel();
				int vp = top.getVisitedPoint();
				if (level + cand.size() <= MaxOne) {
					continue;
				}
/*				if (allContained(cand, notset)) {
					continue;
				}
*/				if (result.size() + 1 == level) {
					result.add(vp);
				} else {
					result.set(level - 1, vp);
				}
				if (cand.isEmpty()) {
					if (notset.isEmpty()) {
						emitClique(result, level, cand, context);
					}
					continue;
				}
				int fixp = findMaxDegreePoint(cand);
				ArrayList<Integer> noneFixp = new ArrayList<Integer>(
						cand.size() - maxdeg);
				HashMap<Integer, Integer> tmpcand = genInterSet(cand, fixp,
						maxdeg, noneFixp);
				HashSet<Integer> tmpnot = genInterSet(notset, fixp);
				stack.add(new CPD(fixp, level + 1, tmpcand, tmpnot));
				notset.add(fixp);
				for (int fix : noneFixp) {
					HashMap<Integer, Integer> tcd = genInterSet(cand, fix);
					HashSet<Integer> tnt = genInterSet(notset, fix);
					stack.add(new CPD(fix, level + 1, tcd, tnt));
					notset.add(fix);
				}
			}
		}

		private void emitClique(ArrayList<Integer> result2, int level,
				HashMap<Integer, Integer> cand, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder sb = new StringBuilder();
			for (int i = 1; i < level; i++) {
				sb.append(result.get(i)).append(" ");
			}
			for (int i : cand.keySet()) {
				sb.append(i).append(" ");
			}
			context.write(new IntWritable(result.get(0)),
					new Text(sb.toString()));
		
		}

		private HashSet<Integer> genInterSet(HashSet<Integer> notset, int aim) {
			HashSet<Integer> result = new HashSet<Integer>();
			HashSet<Integer> adj = verEdge.get(aim);
			if (adj.size() > notset.size()) {
				for (int i : notset) {
					if (adj.contains(i))
						result.add(i);
				}
			} else {
				for (int i : adj) {
					if (notset.contains(i))
						result.add(i);
				}
			}
			return result;
		}

		private HashMap<Integer, Integer> genInterSet(
				HashMap<Integer, Integer> cand, int aim, int maxdeg,
				ArrayList<Integer> noneFixp) {
			HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();
			int acc = 0;
			HashSet<Integer> adj = verEdge.get(aim);
			cand.remove(aim);
			Iterator<Integer> it = cand.keySet().iterator();
			int tmp;
			while (acc < maxdeg && it.hasNext()) {
				tmp = it.next();
				if (adj.contains(tmp)) {
					result.put(tmp, 0);
				} else {
					noneFixp.add(tmp);
				}
			}
			while (it.hasNext())
				noneFixp.add(it.next());
			return result;
		}

		private int findMaxDegreePoint(HashMap<Integer, Integer> cand) {
			int maxpoint = 0, tmpdeg = 0;
			maxdeg = -1;

			for (Map.Entry<Integer, Integer> en : cand.entrySet()) {
				HashSet<Integer> adj = verEdge.get(en.getKey());
				tmpdeg = computeDeg(adj, cand.keySet());
				if (tmpdeg > maxdeg) {
					maxdeg = tmpdeg;
					maxpoint = en.getKey();
				}
			}
			return maxpoint;
		}

		private int computeDeg(HashSet<Integer> adj, Set<Integer> keySet) {
			int deg = 0;
			if (adj.size() > keySet.size()) {
				for (int k : keySet) {
					if (adj.contains(k))
						deg++;
				}
			} else {
				for (int k : adj) {
					if (keySet.contains(k))
						deg++;
				}
			}
			return deg;
		}

		private void readVerEdge(String s,
				HashMap<Integer, HashSet<Integer>> edge) {
			String[] items = s.split(" ");
			for (String item : items) {
				String[] abs = item.split("=");
				int key = Integer.parseInt(abs[0]);
				String[] values = abs[1].split(",");
				HashSet<Integer> adjs = new HashSet<Integer>();
				for (String value : values)
					adjs.add(Integer.parseInt(value));
				edge.put(key, adjs);
			}
		}

		private boolean allContained(HashMap<Integer, Integer> cand,
				HashSet<Integer> notset) {
			for (int nt : notset) {
				HashSet<Integer> nadj = verEdge.get(nt);
				if (nadj.containsAll(cand.keySet()))
					return true;
			}
			return false;
		}
	}
	public static class BottleneckPartitioner extends Partitioner<PairTypeInt,Text> {

		@Override
		public int getPartition(PairTypeInt key, Text value, int num) {
			return (key.getB())%num;//平均分配到各个计算节点之上
		}

	}
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 3) {
	      System.err.println("Usage: CliqueMain <in> <out> <reducenum>");
	      System.exit(2);
	    }
		String in=args[0];
		String pre=args[1];
		int reducenum=Integer.valueOf(args[2]);
		
		
		Job job = new Job(conf,"their bottle neck clique");	
		job.setJarByClass(BottleneckDetect.class);
		job.setMapperClass(BottleneckDetectMapper.class);
		job.setPartitionerClass(BottleneckPartitioner.class);
		job.setReducerClass(BKPBReducer.class);
		
		job.setMapOutputKeyClass(PairTypeInt.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(reducenum);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(pre+"_result_bkpb"));
		long t1 = System.currentTimeMillis();
		job.waitForCompletion(true);
		long t2 = System.currentTimeMillis();
		System.out.println(pre+"-phase cost:"+ (t2-t1));
	}

}

