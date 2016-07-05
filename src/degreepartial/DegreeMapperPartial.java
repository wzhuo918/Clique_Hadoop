package degreepartial;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pairtype.PairType;

public class DegreeMapperPartial extends Mapper<LongWritable,Text,LongWritable,PairType> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
    	StringTokenizer str = new StringTokenizer(value.toString());
    	//String str=value.toString();
    	//String[] stre = str.split(",");
		long p1=-1;
		long p2=-1;
		//p1 = Long.valueOf(stre[0]);
		//p2 = Long.valueOf(stre[1]);		
		if(str.hasMoreElements())
		{
			p1=Long.valueOf(str.nextToken());
			if(str.hasMoreElements())
				p2=Long.valueOf(str.nextToken());
		}
		
		context.write(new LongWritable(p1), new PairType(p1,p2));
		context.write(new LongWritable(p2), new PairType(p1,p2));
    }
}
