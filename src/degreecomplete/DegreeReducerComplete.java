package degreecomplete;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import pairtype.PairType;


public class DegreeReducerComplete extends Reducer<PairType,PairType,PairType,PairType>
{

	@Override
	protected void reduce(PairType key, Iterable<PairType> values,Context context)
			throws IOException, InterruptedException {
		//一条边的两个节点度数合并((1,2),(3,_),((1,2),(_,4))
		//合并为((1,2),(3,4))
		long d1=-1;
		long d2=-1;// 两个度数
		for(PairType p: values)
		{
			if(p.getA()==-1)
				d2=p.getB();
			else
				d1=p.getA();
		}
		context.write(key, new PairType(d1,d2));
	}

}
