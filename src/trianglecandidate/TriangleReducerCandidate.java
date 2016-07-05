package trianglecandidate;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pairtype.PairType;


public class TriangleReducerCandidate extends Reducer<LongWritable,PairType,PairType,LongWritable>
{
	@Override
	protected void reduce(LongWritable key, Iterable<PairType> values,Context context)
			throws IOException, InterruptedException {
		ArrayList<PairType> ap = new ArrayList<PairType>(250);
		for(PairType p : values)//将迭代器中值拿出，两层遍历
		{
			PairType ptmp = new PairType();
    		ptmp.setA(p.getA());
    		ptmp.setB(p.getB());
    		ap.add(ptmp);
		}
		//一个key可能对应多个value，{1  (1,2)|(1,7)|(1,4)|(1,5)}，
		//两两之间都有可能形成三角形
		//即((2,7),1),((2,4),1)等
		for(int i=0;i<ap.size();i++)
			for(int j=i+1;j<ap.size();j++)
			{
				PairType out = ap.get(i);
				PairType in = ap.get(j);
				long keyOne = out.getPointnotV(key.get());
				long keyTwo = in.getPointnotV(key.get());
				if(keyOne < keyTwo)
					context.write(new PairType(keyOne,keyTwo), key);
				else
					context.write(new PairType(keyTwo,keyOne), key);
			
			}
		
	}
	
}
