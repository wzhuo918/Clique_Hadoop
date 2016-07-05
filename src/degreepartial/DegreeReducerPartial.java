package degreepartial;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import pairtype.PairType;

public class DegreeReducerPartial extends Reducer<LongWritable,PairType,PairType,PairType> {
	//填充度数
	//(1,(1,2)),(1,(1,3)),(1,(4,1))变为
	//((1,2),(3,_)),((1,3),(3,_)),((4,1),(_,3))的形式
    @Override
    protected void reduce(LongWritable key, Iterable<PairType> values, Context context)
                        throws IOException, InterruptedException {
    	int degreeTmp=0;
    	ArrayList<PairType> ap = new ArrayList<PairType>(400);
    	for(PairType p : values)
    	{
    		degreeTmp++;
    		PairType ptmp = new PairType();
    		ptmp.setA(p.getA());
    		ptmp.setB(p.getB());
    		ap.add(ptmp);
    	}
        for(PairType p : ap)
        {
        	if(key.get() == p.getA())
        		context.write(p, new PairType(degreeTmp,-1));
        	else
        		context.write(p, new PairType(-1,degreeTmp));
        }
    }
}
