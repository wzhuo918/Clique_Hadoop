package triangleresult;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pairtype.PairType;


public class TriangleMapperResult extends Mapper<LongWritable,Text,PairType,Text>
{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
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
		context.write(new PairType(Long.valueOf(p1[0]),Long.valueOf(p1[1])),
					new Text(s2));


	}
	
}
