package step1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

import main.RunOver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//import cliquenew.VariableRecord;

public class DetectTimeAll {
	public static HashMap<Integer, Integer> vertex = new HashMap<Integer, Integer>(
			3000);
	public static HashMap<Integer, HashSet<Integer>> verEdge = new HashMap<Integer, HashSet<Integer>>(
			3000);
	// public static HashMap<Integer, Integer> mark = new HashMap<Integer,
	// Integer>();
	public static ArrayList<Integer> result = new ArrayList<Integer>(30);
	public static long number = 0;
	public static int levelNumber = 0;
	public static int totalPart = 36;
	public static int tmpKey = 0;
	public static int count = 0; 
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
	public static BufferedWriter raf = null;
	public static BufferedWriter rnew = null;
	public static int which = 0;// 区分reduce的唯一编号，用于写数据文件
	public static int MaxOne = 3;// 最小是三角形，初始化时即初始化为三角形
	public static int randomSelect = 0;
	public static HashSet<Integer> parts = new HashSet<Integer>();
	public static int numReducer = 0;
	public static File curReduce;
	public static class DetectMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		IntWritable ok = new IntWritable();
		Text ov = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] inputs = value.toString().split("\t");
			ok.set(Integer.parseInt(inputs[0]));
			ov.set(inputs[1]);
			context.write(ok, ov);
		}
	}

	public static class DetectReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		//HashSet<Integer> notset = new HashSet<Integer>();
		//HashMap<Integer, Integer> cand = new HashMap<Integer, Integer>();
		HashMap<Integer, HashSet<Integer>> deg2cand = new HashMap<Integer, HashSet<Integer>>();
		TreeMap<Integer, HashSet<Integer>> od2 = new TreeMap<Integer, HashSet<Integer>>();

		private int maxdeg;
		private boolean isClique;


		long allStart = 0;
		long allEnd = 0;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			numReducer = context.getNumReduceTasks();
			count = (int)Math.random()*numReducer;
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
			curReduce = new File(dirRoot, "/outresult/hybrid/" + which);
			raf = new BufferedWriter(new FileWriter(curReduce));

			allStart = System.currentTimeMillis();
		}
		long t1;
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			t1 = System.currentTimeMillis();
			/**
			System.out.println("prepare cost:" + preparetime);*/
			System.out.println("in clean up START time is out:"
					+ (tPhase > TimeThreshold) + "\ttPhase is " + tPhase
					+ "\tTimeThreshold is " + TimeThreshold);
			
			// reduce运行结束，时间T还没有到，接着读取/home/dic/over/中的文件处理
			raf.flush();
			raf.close();
			if (curReduce.length() == 0) {
				File f = new File(dirRoot, "/outresult/hybrid/" + which + "");
				boolean RES = f.delete();
				System.out.println("this reducer is number:" + which + " "
						+ "and raf is empty so delete file:" + f.getPath()
						+ " --" + RES);
			}else if (curReduce.length() != 0 && tPhase < TimeThreshold) {// 文件中有内容
				
				Stack<Status> stack = new Stack<Status>();
				verEdge.clear();
				stack.clear();
				result.clear();
				BufferedReader raf = new BufferedReader(new FileReader(curReduce));
				// 重新写入这个文件，原来的文件作废，最后将其删除！
				rnew = new BufferedWriter(new FileWriter(dirRoot+
						"/outresult/hybrid/" + which + "#"));
				String line = "";
				long t2 = System.currentTimeMillis();
			    
				////
//				while(true){
					
					while ((line = raf.readLine()) != null) {
						
						// 是子图或者边邻接信息
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
							Status st = new Status(elements[3]);
							stack.add(st);
							if (elements[0].equals("0")) {
								// 完整的子图包括verEdge
								readVerEdge(elements[4], verEdge);
							} else {
								// 仅子图信息
								continue;// 还没有读全，继续往下读
							}
						}
						// 一个完整的子图已经都读进来了，计算这个子图
						//System.out.println("read in a hole sub graph");

						balanceOrNot = false;
						parts.clear();
						while (!stack.empty()) {
							Status top = stack.pop();
							HashSet<Integer> notset = top.getNotset();
							HashMap<Integer, Integer> candset = top.getCandidate();
							HashMap<Integer, HashSet<Integer>> d2c;
							TreeMap<Integer, HashSet<Integer>> od2c;
							int vp = top.getVp();
							int level = top.getLevel();
							if(top.getResult()!=null){
								result = top.getResult();
							}
							if (level + candset.size() <= MaxOne) {
								continue;
							}
							if (allContained(candset, notset)) {
								continue;
							}
							if (result.size() + 1 == level) {
								result.add(vp);
							} else {
								result.set(level - 1, vp);
							}	
//							if (candset.isEmpty()) {
//								if (notset.isEmpty()) {
//									emitClique(result, level, candset, context);
//								}
//								continue;
//							}
							
							d2c = top.getDeg2cand();
							od2c = top.getOd2c();
							if (d2c == null) {
								d2c = new HashMap<Integer, HashSet<Integer>>();
								od2c = new TreeMap<Integer, HashSet<Integer>>();
								updateDeg(candset, d2c, od2c);
							}
							if (judgeClique(d2c)) {
								emitClique(result, level, candset, context);
								continue;
							}else{
								int aim = 0, mindeg = Integer.MAX_VALUE;
								while (candset.size() + level > MaxOne + 1
										&& candset.size() > 0) {
									//System.out.println("cutt "+tmpKey);
									Map.Entry<Integer, HashSet<Integer>> firstEntry = od2c
											.firstEntry();
									aim = firstEntry.getValue().iterator().next();//
									mindeg = firstEntry.getKey();
									
									if (mindeg == 0) {
										HashSet<Integer> cds = firstEntry.getValue();
										if (level < MaxOne) {
											for (Integer integer : cds) {
												candset.remove(integer);
											}			
										}else {
											for (Integer integer : cds) {
												if (genInterSet(notset, integer).size() == 0) {
													emitClique(result, level, integer, context);
													notset.add(integer);
												}
												candset.remove(integer);
											}
										}
										d2c.remove(mindeg);
										od2c.remove(mindeg);
									}
									
									else{
										int maxDeg = od2c.lastEntry().getKey();
										if (maxDeg>=0.8*candset.size()) {
//											int fixp = findMaxDegreePoint(candset);
											int fixp = od2c.lastEntry().getValue().iterator().next();
											ArrayList<Integer> noneFixp = new ArrayList<Integer>(
													candset.size() - maxDeg);
											HashMap<Integer, Integer> tmpcand = genInterSet(candset, fixp,
												maxDeg, noneFixp);//
											HashSet<Integer> tmpnot = genInterSet(notset, fixp);
											Status tmp = new Status(fixp, level + 1, tmpcand, tmpnot, null, null);
											
											t1 = splitBigGraph(tmp, context, "cleanup");
																						
											notset.add(fixp);
											
											for (int fix : noneFixp) {
												HashMap<Integer, Integer> tempcand = genInterSet(candset, fix);
												HashSet<Integer> tempnot = genInterSet(notset, fix);
												Status temp = new Status(fix, level + 1, tempcand, tempnot, null, null);
												if(tPhase < TimeThreshold){
													if (tempcand.size()<=sizeN) {
														computeSmallGraph(temp, context, 0);
													}else {
														stack.add(temp);
													}
												}else {
													spillToDisk(temp, rnew);
												}
												
												notset.add(fix);
											}
											break;
										}else{
											HashMap<Integer, Integer> aimSet = updateMarkDeg(
													aim, mindeg, candset, d2c, od2c);
											HashSet<Integer> aimnotset = genInterSet(notset,
													aim);
											Status ss = new Status(aim, level + 1, aimSet,
													aimnotset, null, null);
											if (tPhase < TimeThreshold) {
												if (aimSet.size() <= sizeN) {
													computeSmallGraph(ss, context, 1);
												} else {
													stack.add(ss);
												}
											}else {
												spillToDisk(ss, rnew);
											}
											notset.add(aim);
										}
									}
									
									if (judgeClique(d2c)) {
										if (candset.size() > 0) {
											Map.Entry<Integer, HashSet<Integer>> lastEntry = od2c
													.lastEntry();
											aim = lastEntry.getValue().iterator()
													.next();
											notset.retainAll(verEdge.get(aim));
										}
										if (level + candset.size() <= MaxOne) {
											break;
										}
										if (allContained(candset, notset)) {
											break;
										} else {
											emitClique(result, level, candset, context);
											break;
										}
									}
									if (tPhase < TimeThreshold) {
										t2 = System.currentTimeMillis();
										tPhase += (t2 - t1);
										t1 = t2;// 重新设定累计起点
									}
							    }
								if (tPhase < TimeThreshold) {
									t2 = System.currentTimeMillis();
									tPhase += (t2 - t1);
									t1 = t2;// 重新设定累计起点
								}else{
									break;
								}
						    }
						}//stack不空
						while (!stack.empty()) {
							// 退出了栈还没空，说明时间到了还没计算完，把栈中的子图spill到磁盘
							spillToDisk(stack.pop(), rnew);
						}
						if (balanceOrNot) {
							rnew.write(((-2) + "\t" + 1 + "#") );
							String pas = parts.toString();
							rnew.write(pas.substring(1, pas.length() - 1)
									 );
							rnew.write(("#" + tmpKey + "#") );
							writeVerEdge(verEdge, rnew);
							rnew.write(("\n") );// rnew.write(("\n0\t0\n") );
						}
						if (tPhase >= TimeThreshold) {
							/**
							System.out.println("in clean up OUTER time is out:"
									+ (tPhase > TimeThreshold) + "\tt is " + tPhase
									+ "\tpahse is " + TimeThreshold);*/
							break;
						}// 超时之后，后面文件也不读了
					}// while
					
					System.out.println("append left file to end");
					while ((line = raf.readLine()) != null) {
						// 把原文件后面的内容直接考到新文件后面
						rnew.write(line );
						rnew.write("\n" );
					}
					raf.close();
					File endf = new File(dirRoot, "/outresult/hybrid/" + which + "");
					boolean endRES = endf.delete();
					System.out.println("this reducer is number:" + which + " "
							+ "and clean up to end, so delete file:"
							+ endf.getPath() + " --" + endRES);
					rnew.close();
					File tf = new File(dirRoot, "/outresult/hybrid/" + which
							+ "#");
					if (tf.length() == 0) {

						boolean ntr = tf.delete();
						System.out
								.println("this reducer is number:"
										+ which
										+ " "
										+ "and clean up to end & rnew is empty, so delete file:"
										+ tf.getPath() + " --" + ntr);
//						break;
					}
/*					if (tPhase < TimeThreshold) {
						t2 = System.currentTimeMillis();
						tPhase += (t2 - t1);
						t1 = t2;// 重新设定累计起点
					}else{
						break;
					}
					tf.renameTo(new File(dirRoot, "/outresult/hybrid/" + which));
					raf = new BufferedReader(new FileReader(tf));
					rnew = new BufferedWriter(new FileWriter(dirRoot+
						"/outresult/hybrid/" + which + "#"));
*/						
//				}//while out
				
				
				
			}											
				
			super.cleanup(context);

			allEnd = System.currentTimeMillis();
			System.out.println("all time: "+(allEnd - allStart)/1000);
		}

		private void writeVerEdge(HashMap<Integer, HashSet<Integer>> edge,
				BufferedWriter rf) throws IOException {
			for (Map.Entry<Integer, HashSet<Integer>> en : edge.entrySet()) {
				// rf.write(en.getKey());
				rf.write((en.getKey() + "=") );
				Iterator<Integer> it = en.getValue().iterator();
				rf.write(it.next().toString() );
				while (it.hasNext()) {
					rf.write(("," + it.next()) );
				}
				rf.write(" " );
			}
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

		private void readVerEdge(String s,
				HashMap<Integer, HashSet<Integer>> edge, int i) {
			String[] items = s.split("], ");
			for (String item : items) {
				String[] abs = item.split("=");
				int key = Integer.parseInt(abs[0]);
				String[] values = abs[1].substring(1, abs[1].length()).split(
						", ");
				HashSet<Integer> adjs = new HashSet<Integer>();
				for (String value : values)
					adjs.add(Integer.parseInt(value.endsWith("]") ? value
							.substring(0, value.length() - 1) : value));
				edge.put(key, adjs);
			}
		}

		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			t1 = System.currentTimeMillis();

			tmpKey = key.get();

			if (thenode.contains(tmpKey)) {
//			if(true){
//			if(tmpKey == 123){
				

				NodeSN++;
				if (tPhase < TimeThreshold) // 时间小于阈值
				{
					vertex.clear();
					verEdge.clear();
					parts.clear();
					count = 0;
					//notset.clear();
					//cand.clear();
					deg2cand.clear();
					od2.clear();
					number = 0;
					triangleNumber = 0;// 每次初始化
					cutNumber = 0;
					cutNumberAfter = 0;
					balanceOrNot = false;
					for (Text t : values) {
						String ver = t.toString();
						triangleNumber++;
						String[] edgepoint = ver.split(",");
						int node1 = Integer.valueOf(edgepoint[0]);
						int node2 = Integer.valueOf(edgepoint[1]);
						// 顶点度数统计，暂时可能用不上，记录，若成为耗时一环再删除，单只记录顶点
						if (vertex.containsKey(node1)) {
							vertex.put(node1, vertex.get(node1) + 1);
						} else {
							vertex.put(node1, 1);
						}
						if (vertex.containsKey(node2)) {
							vertex.put(node2, vertex.get(node2) + 1);
						} else {
							vertex.put(node2, 1);
						}
						// 顶点相邻边的统计
						HashSet<Integer> tmp = verEdge.get(node1);
						if (tmp != null) {
							tmp.add(node2);
							// verEdge.put(node1, tmp);
						} else {
							tmp = new HashSet<Integer>();
							// tmp.add(node1);
							tmp.add(node2);
							verEdge.put(node1, tmp);
						}
						tmp = verEdge.get(node2);
						if (tmp != null) {
							tmp.add(node1);
							// verEdge.put(node2, tmp);
						} else {
							tmp = new HashSet<Integer>();
							tmp.add(node1);
							verEdge.put(node2, tmp);
						}
					}

					Iterator<Map.Entry<Integer, Integer>> iter = vertex
							.entrySet().iterator();
					HashMap<Integer, Integer> tcand = new HashMap<Integer, Integer>();
					HashSet<Integer> tnot = new HashSet<Integer>();
					while (iter.hasNext()) {
						Map.Entry<Integer, Integer> et = iter.next();
						if (et.getKey() < tmpKey) {
							tnot.add(et.getKey());
						} else{
							tcand.put(et.getKey(), et.getValue());
							HashSet<Integer> keyset = deg2cand.get(et
									.getValue());
							if (keyset == null) {
								keyset = new HashSet<Integer>();
								deg2cand.put(et.getValue(), keyset);
								od2.put(et.getValue(), keyset);
							}
							keyset.add(et.getKey());
						}
					}

					if (tcand.size() < MaxOne)
						return;
						
					//long t2 = System.currentTimeMillis();
					Status s = new Status(tmpKey, 1, tcand, tnot, null,
							null);

					Stack<Status> stack = new Stack<Status>();
					stack.add(s);
					
					while (!stack.empty()) {
						Status top = stack.pop();
						HashSet<Integer> notset = top.getNotset();
						HashMap<Integer, Integer> candset = top.getCandidate();
						int vp = top.getVp();
						int level = top.getLevel();
						HashMap<Integer, HashSet<Integer>> d2c;
						TreeMap<Integer, HashSet<Integer>> od2c;
						
						if (level + candset.size() <= MaxOne) {
							continue;
						}
						
						if (allContained(candset, notset)) {
							continue;
						}
						
						if (result.size() + 1 == level) {
							result.add(vp);
						} else {
							result.set(level - 1, vp);
						}
						
/*						
						if (candset.isEmpty()) {
							if (notset.isEmpty()) {
								emitClique(result, level, candset, context);
							}
							continue;
						}
*/						
						d2c = top.getDeg2cand();
						od2c = top.getOd2c();
						if (d2c == null) {
							d2c = new HashMap<Integer, HashSet<Integer>>();
							od2c = new TreeMap<Integer, HashSet<Integer>>();
							updateDeg(candset, d2c, od2c);
						}
						
						if (judgeClique(d2c)) {
							emitClique(result, level, candset, context);
							continue;
						}else{
							
							int aim = 0, mindeg = Integer.MAX_VALUE;
							while (candset.size() + level > MaxOne + 1
									&& candset.size() > 0) {
								//System.out.println("cutt "+tmpKey);
								Map.Entry<Integer, HashSet<Integer>> firstEntry = od2c
										.firstEntry();
								aim = firstEntry.getValue().iterator().next();//
								mindeg = firstEntry.getKey();
								
								if (mindeg == 0) {
									HashSet<Integer> cds = firstEntry.getValue();
									if (level < MaxOne) {
										for (Integer integer : cds) {
											candset.remove(integer);
										}			
									}else {
										for (Integer integer : cds) {
											if (genInterSet(notset, integer).size() == 0) {
												emitClique(result, level, integer, context);
												notset.add(integer);
											}
											candset.remove(integer);
										}
									}
									d2c.remove(mindeg);
									od2c.remove(mindeg);
								}
								
								else{
								
									int maxDeg = od2c.lastEntry().getKey();
									if (maxDeg>0.8*candset.size()) {
//										int fixp = findMaxDegreePoint(candset);	
										int fixp = od2c.lastEntry().getValue().iterator().next();
																			
										ArrayList<Integer> noneFixp = new ArrayList<Integer>(
												candset.size() - maxDeg);
										HashMap<Integer, Integer> tmpcand = genInterSet(candset, fixp,
											maxDeg, noneFixp);//
										HashSet<Integer> tmpnot = genInterSet(notset, fixp);
										Status tmp = new Status(fixp, level + 1, tmpcand, tmpnot, null, null);										
										
										t1 = splitBigGraph(tmp, context, "reduce");																		
										
										notset.add(fixp);
										
										for (int fix : noneFixp) {
											
											HashMap<Integer, Integer> tempcand = genInterSet(candset, fix);
											HashSet<Integer> tempnot = genInterSet(notset, fix);
											Status temp = new Status(fix, level + 1, tempcand, tempnot, null, null);
									
											if (tempcand.size()<=sizeN && (tPhase<TimeThreshold)) {
												computeSmallGraph(temp, context, 0);
											}else {
												stack.add(temp);
											}
											notset.add(fix);
										}
										
										break;
										
									}else{
										HashMap<Integer, Integer> aimSet = updateMarkDeg(
												aim, mindeg, candset, d2c, od2c);
										HashSet<Integer> aimnotset = genInterSet(notset,
												aim);
										Status ss = new Status(aim, level + 1, aimSet,
												aimnotset, null, null);
										if (aimSet.size() <= sizeN && (tPhase < TimeThreshold)) {
											computeSmallGraph(ss, context, 1);
										} else {
											spillToDisk(ss, raf);
										}
										notset.add(aim);
									}
								}//else
								
								if (judgeClique(d2c)) {
									if (candset.size() > 0) {
										Map.Entry<Integer, HashSet<Integer>> lastEntry = od2c
												.lastEntry();
										aim = lastEntry.getValue().iterator()
												.next();
										notset.retainAll(verEdge.get(aim));
									}
									if (level + candset.size() <= MaxOne) {
										break;
									}
									if (allContained(candset, notset)) {
										break;
									} else {
										emitClique(result, level, candset, context);
										break;
									}
								}
								if (tPhase < TimeThreshold) {
									long t2 = System.currentTimeMillis();
									tPhase += (t2 - t1);
									t1 = t2;// 重新设定累计起点
								}
								
						    }//while
							if (tPhase < TimeThreshold) {
								long t2 = System.currentTimeMillis();
								tPhase += (t2 - t1);
								t1 = t2;// 重新设定累计起点
							}else{
								break;
							}
					    }
				    }//else
					while(!stack.empty()){
						spillToDisk(stack.pop(),raf);
					}
					if (balanceOrNot) {
						// 若份数比totalPart大则每个reduce发一份，否则只发对应份
						// 记录最大，每个计算节点发一份
						raf.write(((-2) + "\t" + 1 + "#") );
						String pas = parts.toString();
						raf.write(pas.substring(1, pas.length() - 1) );
						raf.write(("#" + tmpKey + "#") );
						writeVerEdge(verEdge, raf);
						raf.write(("\n") );
						// raf.write(("\n0\t0\n") );
					}
				} else// 时间超阈值，状态直接发往下一个phase
				{
					vertex.clear();
					verEdge.clear();
//					System.out.println("time is out so generate size-1 graph for node"+tmpKey);
					
					for (Text t : values) {
						
						String ver = t.toString();
						triangleNumber++;
						String[] edgepoint = ver.split(",");
						int node1 = Integer.valueOf(edgepoint[0]);
						int node2 = Integer.valueOf(edgepoint[1]);
						// 顶点度数统计，暂时可能用不上，记录，若成为耗时一环再删除，单只记录顶点
						vertex.put(node1, 1);
						vertex.put(node2, 1);
						// 顶点相邻边的统计
						HashSet<Integer> tmp = verEdge.get(node1);
						if (tmp != null) {
							tmp.add(node2);
							// verEdge.put(node1, tmp);
						} else {
							tmp = new HashSet<Integer>();
							// tmp.add(node1);
							tmp.add(node2);
							verEdge.put(node1, tmp);
						}
						tmp = verEdge.get(node2);
						if (tmp != null) {
							tmp.add(node1);
							// verEdge.put(node2, tmp);
						} else {
							tmp = new HashSet<Integer>();
							tmp.add(node1);
							verEdge.put(node2, tmp);
						}
					}
					//System.out.println("time is out! during generating");
					Iterator<Map.Entry<Integer, Integer>> iter = vertex
							.entrySet().iterator();
					HashMap<Integer, Integer> tcand = new HashMap<Integer, Integer>();
					HashSet<Integer> tnot = new HashSet<Integer>();
					while (iter.hasNext()) {
						Map.Entry<Integer, Integer> et = iter.next();
						if (et.getKey() < tmpKey) {
							tnot.add(et.getKey());
						} else{
							tcand.put(et.getKey(), et.getValue());
						}
					}

					if (tcand.size() < MaxOne)
						return;
					Status top = new Status(tmpKey, 1, tcand, tnot, null,
							null);
					
					// 只要有数据写到磁盘且未完成计算就需要多一个MR步骤
					// 整个状态发往下一个步骤，发往节点为p，边连接情况也只用发往p
					int p = NodeSN % numReducer;
					raf.write(((-1) + "\t" + 0 + "@" + p + "@" + tmpKey + "@"
							+ top.toString(result) + "@") );
					writeVerEdge(verEdge, raf);
					raf.write(("\n") );
				}// 计算末尾
				if (tPhase < TimeThreshold) {
					long t2 = System.currentTimeMillis();
					tPhase += (t2 - t1);
					t1 = t2;// 重新设定累计起点
				}
			}// if该节点需要计算
		}// reduce

		private long splitBigGraph(Status sst, Context context, String position)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		long tSplitStart = System.currentTimeMillis();
		long tSplitMedium;
		Status bigStatus = sst;
		Stack<Status> smallStack = new Stack<Status>();
		smallStack.add(sst);
		while (!smallStack.empty()) {
			Status s = smallStack.pop();
			if(tPhase >= TimeThreshold){
				if(s != bigStatus){
					if(position.equals("reduce")){
						spillToDisk(s,raf);
					}else{
						spillToDisk(s,rnew);
					}
				    tSplitMedium = System.currentTimeMillis();
					tPhase += (tSplitMedium - tSplitStart);
					tSplitStart = tSplitMedium;// 重新设定累计起点				
					continue;
				}
			}
			HashSet<Integer> notset = s.getNotset();
			HashMap<Integer, Integer> cand = s.getCandidate();
			int vp = s.getVp();
			int level = s.getLevel();
			if (level + cand.size() <= MaxOne) {
				continue;
			}
			if (allContained(cand, notset)) {
				continue;
			}
			
			if (result.size() + 1 == level) {
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
			// int maxdeg = cand.get(fixp);
/*					
			if (isClique) {
				emitClique(result, level, cand, context);
				continue;
			}
*/					
			ArrayList<Integer> noneFixp = new ArrayList<Integer>(
					cand.size() - maxdeg);
			HashMap<Integer, Integer> tmpcand = genInterSet(cand, fixp,
					maxdeg, noneFixp);//
			HashSet<Integer> tmpnot = genInterSet(notset, fixp);
			Status tmp = new Status(fixp, level + 1, tmpcand, tmpnot,
					null, null);
			
			if(bigStatus == s){
				bigStatus = tmp;
			}
			smallStack.add(tmp);
			notset.add(fixp);
			
			for (int fix : noneFixp) {
				HashMap<Integer, Integer> tcand = genInterSet(cand, fix);
				HashSet<Integer> tnot = genInterSet(notset, fix);
				Status temp = new Status(fix, level + 1, tcand, tnot,
						null, null);
				if(tPhase < TimeThreshold){
					if(tcand.size()<=sizeN){
						computeSmallGraph(temp, context, 0);
					}else{
						smallStack.add(temp);
					}
				}else{
					if(position.equals("reduce")){
						spillToDisk(temp,raf);
					}else{
						spillToDisk(temp,rnew);
					}
				}
				
				notset.add(fix);
			}
			
			tSplitMedium = System.currentTimeMillis();
			tPhase += (tSplitMedium - tSplitStart);
			tSplitStart = tSplitMedium;// 重新设定累计起点
			
		}
		tSplitMedium = System.currentTimeMillis();
		tPhase += (tSplitMedium - tSplitStart);
		return tSplitMedium;// 重新设定累计起点

	}
			
		private void computeSmallGraph(Status sst, Context context, int type)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Stack<Status> smallStack = new Stack<Status>();
			smallStack.add(sst);
			switch (type) {
			case 0:// 小的子图用bkpb的方法算
				while (!smallStack.empty()) {
					Status s = smallStack.pop();
					HashSet<Integer> notset = s.getNotset();
					HashMap<Integer, Integer> cand = s.getCandidate();
					int vp = s.getVp();
					int level = s.getLevel();
					if (level + cand.size() <= MaxOne) {
						continue;
					}
					if (allContained(cand, notset)) {
						continue;
					}
					
					if (result.size() + 1 == level) {
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
					// int maxdeg = cand.get(fixp);
/*					
					if (isClique) {
						emitClique(result, level, cand, context);
						continue;
					}
*/				
					ArrayList<Integer> noneFixp = new ArrayList<Integer>(
							cand.size() - maxdeg);
					HashMap<Integer, Integer> tmpcand = genInterSet(cand, fixp,
							maxdeg, noneFixp);//
					HashSet<Integer> tmpnot = genInterSet(notset, fixp);
					Status tmp = new Status(fixp, level + 1, tmpcand, tmpnot,
							null, null);
				
					smallStack.add(tmp);
					notset.add(fixp);
					
					for (int fix : noneFixp) {
						HashMap<Integer, Integer> tcand = genInterSet(cand, fix);
						HashSet<Integer> tnot = genInterSet(notset, fix);
						Status temp = new Status(fix, level + 1, tcand, tnot,
								null, null);
					
						smallStack.add(temp);
						notset.add(fix);
					}
					//看到这儿了
				}
				break;
			case 1:// 小的子图用binary的方法算
				while (!smallStack.empty()) {
					Status top = smallStack.pop();
					HashSet<Integer> notset = top.getNotset();
					HashMap<Integer, Integer> cand = top.getCandidate();
					HashMap<Integer, HashSet<Integer>> d2c;
					TreeMap<Integer, HashSet<Integer>> od2c;
					int level = top.getLevel();
					int vp = top.getVp();
					if (level + cand.size() <= MaxOne
							|| allContained(cand, notset)) {
						continue;
					}
					if (result.size() + 1 == level) {
						result.add(vp);
					} else {
						result.set(level - 1, vp);
					}
					d2c = top.getDeg2cand();
					od2c = top.getOd2c();
					if (d2c == null) {
						d2c = new HashMap<Integer, HashSet<Integer>>();
						od2c = new TreeMap<Integer, HashSet<Integer>>();
						updateDeg(cand, d2c, od2c);
					}
					if (judgeClique(d2c)) {
						emitClique(result, level, cand, context);
						continue;
					} else {
						int aim = 0, mindeg = Integer.MAX_VALUE;
						while (cand.size() + level > MaxOne + 1
								&& cand.size() > 0) {
							Map.Entry<Integer, HashSet<Integer>> firstEntry = od2c
									.firstEntry();
							aim = firstEntry.getValue().iterator().next();//
							mindeg = firstEntry.getKey();
							
							HashMap<Integer, Integer> aimSet = updateMarkDeg(
									aim, mindeg, cand, d2c, od2c);
							HashSet<Integer> aimnotset = genInterSet(notset,
									aim);
							Status ss = new Status(aim, level + 1, aimSet,
									aimnotset, null, null);
							smallStack.add(ss);
							notset.add(aim);
							
							if (judgeClique(d2c)) {
								if (cand.size() > 0) {
									Map.Entry<Integer, HashSet<Integer>> lastEntry = od2c
											.lastEntry();
									aim = lastEntry.getValue().iterator()
											.next();
									notset.retainAll(verEdge.get(aim));
								}
								if (level + cand.size() <= MaxOne) {
									break;
								}
								if (allContained(cand, notset)) {
									break;
								} else {
									emitClique(result, level, cand, context);
									break;
								}
							}
						}
					}
				}
				break;
			}

		}

		private int findMaxDegreePoint(HashMap<Integer, Integer> cand) {
			int maxpoint = 0, tmpdeg = 0, size = cand.size() - 1;
			maxdeg = -1;
			isClique = true;

			for (Map.Entry<Integer, Integer> en : cand.entrySet()) {
				HashSet<Integer> adj = verEdge.get(en.getKey());
				tmpdeg = computeDeg(adj, cand.keySet());
				en.setValue(tmpdeg);
				if (tmpdeg > maxdeg) {
					maxdeg = tmpdeg;
					maxpoint = en.getKey();
				}
				
				if (isClique && tmpdeg != size) {
					isClique = false;
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

		private void spillToDisk(Status ss, BufferedWriter raf)
				throws IOException {
			// TODO Auto-generated method stub
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
			raf.write(sb.toString() );
		}

		private HashMap<Integer, Integer> updateMarkDeg(int aim, int mindeg,
				HashMap<Integer, Integer> cand,
				HashMap<Integer, HashSet<Integer>> d2c,
				TreeMap<Integer, HashSet<Integer>> od2c) {
			cand.remove(aim);
			HashSet<Integer> li = d2c.get(mindeg);
			li.remove(aim);
			if (li.isEmpty()) {
				d2c.remove(mindeg);
				od2c.remove(mindeg);
			}
			HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();
			int acc = 0;
			HashSet<Integer> adj = verEdge.get(aim);
			if (adj.size() > cand.size()) {
				Iterator<Map.Entry<Integer, Integer>> it = cand.entrySet()
						.iterator();
				while (it.hasNext() && acc < mindeg) {
					Map.Entry<Integer, Integer> en = it.next();
					if (adj.contains(en.getKey())) {
						int point = en.getKey(), deg = en.getValue();
						HashSet<Integer> lis = d2c.get(deg);
						lis.remove(point);
						if (lis.isEmpty()) {
							d2c.remove(deg);
							od2c.remove(deg);
						}
						deg--;
						HashSet<Integer> list = d2c.get(deg);
						if (list == null) {
							list = new HashSet<Integer>();
							d2c.put(deg, list);
							od2c.put(deg, list);
						}
						list.add(point);
						en.setValue(deg);
						acc++;
						result.put(point, 0);
					}
				}
			} else {
				Iterator<Integer> it = adj.iterator();
				while (it.hasNext() && acc < mindeg) {
					int point = it.next();
					// Map.Entry<Integer, Integer> en = cand.
					if (cand.containsKey(point)) {
						int deg = cand.get(point);
						HashSet<Integer> lis = d2c.get(deg);
						lis.remove(point);
						if (lis.isEmpty()) {
							d2c.remove(deg);
							od2c.remove(deg);
						}
						deg--;
						HashSet<Integer> list = d2c.get(deg);
						if (list == null) {
							list = new HashSet<Integer>();
							d2c.put(deg, list);
							od2c.put(deg, list);
						}
						list.add(point);
						cand.put(point, deg);
						acc++;
						result.put(point, 0);
					}
				}
			}
			return result;
		}

		private void emitClique(ArrayList<Integer> result2, int level,
				int zeroPoint, Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			for (int i = 1; i < level; i++) {
				sb.append(result.get(i)).append(" ");
			}
			sb.append(zeroPoint).append(" ");
			context.write(new IntWritable(result.get(0)),
					new Text(sb.toString()));

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

		private boolean judgeClique(HashMap<Integer, HashSet<Integer>> d2c) {
			// TODO Auto-generated method stub
			if (d2c.size() > 1)
				return false;
			if (d2c.size() == 0)
				return true;
			if (d2c.size() == 1) {
				Map.Entry<Integer, HashSet<Integer>> first = d2c.entrySet()
						.iterator().next();
				if (first.getKey() + 1 == first.getValue().size())
					return true;
			}
			return false;
		}

		private void updateDeg(HashMap<Integer, Integer> cand,
				HashMap<Integer, HashSet<Integer>> d2c,
				TreeMap<Integer, HashSet<Integer>> od2c) {
			// TODO Auto-generated method stub
			int deg = 0;
			HashSet<Integer> adj;
			for (Map.Entry<Integer, Integer> en : cand.entrySet()) {
				int cad = en.getKey();
				adj = verEdge.get(cad);
				deg = 0;
				if (adj.size() > cand.size()) {
					for (int k : cand.keySet()) {
						if (adj.contains(k)) {
							deg++;
						}
					}
				} else {
					for (int k : adj) {
						if (cand.containsKey(k)) {
							deg++;
						}
					}
				}
				HashSet<Integer> d2cset = d2c.get(deg);
				if (d2cset == null) {
					d2cset = new HashSet<Integer>();
					d2c.put(deg, d2cset);
					od2c.put(deg, d2cset);
				}
				d2cset.add(cad);
				en.setValue(deg);
			}
		}

		private HashMap<Integer, Integer> genInterSet(
				HashMap<Integer, Integer> cand, int aim, int maxdeg,
				ArrayList<Integer> noneFixp) {
			HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();
			HashSet<Integer> adj = verEdge.get(aim);
			cand.remove(aim);
			Iterator<Integer> it = cand.keySet().iterator();
			int tmp;
			while (it.hasNext()) {
				tmp = it.next();
				if (adj.contains(tmp)) {
					result.put(tmp, 0);
				} else {
					noneFixp.add(tmp);
				}
			}
			/*
			while (it.hasNext())
				noneFixp.add(it.next());
            */
			/**
			 * HashSet<Integer> small ,big; if(adj.size()>cand.size()){ small =
			 * (HashSet<Integer>) cand.keySet(); big = adj; }else{ big =
			 * (HashSet<Integer>) cand.keySet(); small = adj; }
			 * Iterator<Integer> it = small.iterator(); int tmp; while(acc <
			 * maxdeg && it.hasNext()){ tmp = it.next(); if(big.contains(tmp)){
			 * acc++; result.put(tmp, 0); } }
			 */
			return result;
		}

		private HashMap<Integer, Integer> genInterSet(
				HashMap<Integer, Integer> cand, int aim) {
			HashMap<Integer, Integer> result = new HashMap<Integer, Integer>();
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
					result.put(tmp, 0);
				}
			}
			return result;
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

		private boolean allContained(HashMap<Integer, Integer> cand,
				HashSet<Integer> notset) {
			for (int nt : notset) {
				HashSet<Integer> nadj = verEdge.get(nt);
				if (nadj.containsAll(cand.keySet()))
					return true;
			}
			return false;
		}

		public void readInData(String filename) throws NumberFormatException,
				IOException {
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line = "";

			while ((line = reader.readLine()) != null) {
				String[] tp = line.split("\t");
				tmpKey = Integer.parseInt(tp[0]);
				String[] edgepoint = tp[1].split(",");
				int node1 = Integer.valueOf(edgepoint[0]);
				int node2 = Integer.valueOf(edgepoint[1]);
				if (vertex.containsKey(node1)) {
					vertex.put(node1, vertex.get(node1) + 1);
				} else {
					vertex.put(node1, 1);
				}
				if (vertex.containsKey(node2)) {
					vertex.put(node2, vertex.get(node2) + 1);
				} else {
					vertex.put(node2, 1);
				}
				HashSet<Integer> tmp = verEdge.get(node1);
				if (tmp != null) {
					tmp.add(node2);
				} else {
					tmp = new HashSet<Integer>();
					tmp.add(node2);
					verEdge.put(node1, tmp);
				}
				tmp = verEdge.get(node2);
				if (tmp != null) {
					tmp.add(node1);
				} else {
					tmp = new HashSet<Integer>();
					tmp.add(node1);
					verEdge.put(node2, tmp);
				}
			}
		}

		public void readEdgeInfo(String filename) throws IOException {
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line = "";
			line = reader.readLine();
			readVerEdge(line, verEdge, 1);
			for (int k : verEdge.keySet()) {
				vertex.put(k, 0);
			}

		}

		public void test(String file) throws IOException, InterruptedException {
			this.readEdgeInfo(file);
			Iterator<Map.Entry<Integer, Integer>> iter = vertex.entrySet()
					.iterator();
			HashMap<Integer, Integer> tcand = new HashMap<Integer, Integer>();
			HashSet<Integer> tnot = new HashSet<Integer>();
			tmpKey = 133094;
			while (iter.hasNext()) {
				Map.Entry<Integer, Integer> et = iter.next();
				if (et.getKey() < tmpKey) {
					tnot.add(et.getKey());
				} else {
					tcand.put(et.getKey(), et.getValue());
				}
			}
			if (tcand.size() < MaxOne)
				return;
			long t1 = System.currentTimeMillis();
			long t2 = System.currentTimeMillis();
			Status top = new Status(tmpKey, 1, vertex, tnot, null, null);
			this.computeSmallGraph(top, null, 1);

		}

	}// reducer

	public static void main(String[] args) throws NumberFormatException,
			IOException, InterruptedException {
		DetectReducer reducer = new DetectReducer();
		reducer.setup(null);
		reducer.test("/home/youli/CliqueHadoop/test");
	}
}// class