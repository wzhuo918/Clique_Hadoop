package triangleresult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pairtype.PairType;


public class TriangleReducerResult extends Reducer<PairType,Text,LongWritable,PairType>
{

	@Override
	protected void reduce(PairType key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		ArrayList<String> as = new ArrayList<String>(250);
		//(2,7){(2,6)....1,3}
		//如果存在连接边，则全都输出为三角形，否则，结果存入链表
		boolean confirm=false;//确认是否能找到三角形
		long a=key.getA();
		long b=key.getB();
		for(Text t : values)
		{
			Text tmp = new Text(t);
			//找到了(2,7)这种连接边，则加入链表
			String tmps = new String(tmp.toString());
			if(!tmps.contains(","))
			{
				as.add(tmps);
				continue;
			}
			else//存在连接边
			{
				confirm=true;
			}
		}
		ArrayList<Long> al=new ArrayList<Long>(3);
		al.add(a);
		al.add(b);
		if (confirm) 
		{
			for (String s : as) //输出的都是满足三角形形式的的元组
			{
				long sl=Long.valueOf(s);
				al.add(sl);
				Collections.sort(al);
				context.write(new LongWritable(al.get(0)), new PairType(al.get(1),al.get(2)));
				//context.write(new LongWritable(al.get(1)), new PairType(al.get(0),al.get(2)));
				//context.write(new LongWritable(al.get(2)), new PairType(al.get(0),al.get(1)));
				al.remove(sl);
			}
		}

			
	}

}
