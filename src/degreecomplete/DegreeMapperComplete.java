package degreecomplete;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pairtype.PairType;


public class DegreeMapperComplete extends Mapper<LongWritable,Text,PairType,PairType>
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
		String[] p2 = s2.split(",");
		context.write(new PairType(Long.valueOf(p1[0]),Long.valueOf(p1[1])),
				new PairType(Long.valueOf(p2[0]),Long.valueOf(p2[1])));
		
	}
	
}

