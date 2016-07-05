package trianglecandidate;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pairtype.PairType;


public class TriangleMapperCandidate extends Mapper<LongWritable,Text,LongWritable,PairType>
{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//((1,2),(3,4))，第二个括号的为两个顶点的度数值
		//输出(1,(1,2))
		StringTokenizer str = new StringTokenizer(value.toString());
		String s1="";
		String s2="";
		if(str.hasMoreElements())
		{
			s1 = str.nextToken();
			if(str.hasMoreElements())
				s2 = str.nextToken();
		}

		String[] p1 = s1.split(",");
		String[] p2 = s2.split(",");
		long d1=Long.valueOf(p2[0]);
		long d2=Long.valueOf(p2[1]);
		//以度数较小的顶点作为键，该边为值
		if(d1 <= d2)
		{
			context.write(new LongWritable(Long.valueOf(p1[0])), 
					new PairType(Long.valueOf(p1[0]),Long.valueOf(p1[1])));
		}
		else
		{
			context.write(new LongWritable(Long.valueOf(p1[1])), 
					new PairType(Long.valueOf(p1[0]),Long.valueOf(p1[1])));
		}
	}

}
