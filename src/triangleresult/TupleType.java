package triangleresult;
//目前的算法暂时用不上，可能有使用的情况，另作讨论
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import pairtype.PairType;


public class TupleType extends PairType{
	
	protected long pointC=-1;
	public long getC()
	{
		return this.pointC;
	}
	public void setC(long c)
	{
		this.pointC=c;
	}
	public void set(long a,long b,long c)
	{
		this.pointA=a;
		this.pointB=b;
		this.pointC=c;
	}
	public TupleType(){}
	public TupleType(long a,long b,long c)
	{
		this.pointA=a;
		this.pointB=b;
		this.pointC=c;
	}
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeLong(pointC);
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		pointC=in.readLong();
	}
	public int hashCode() {
		return Integer.rotateLeft(String.valueOf(pointA * 13 + pointB*7 + pointC)
				.hashCode(), 7);
	}
	
	public boolean equals(Object tuple) {
		if (tuple instanceof TupleType) {
			TupleType t = (TupleType) tuple;
			return t.pointA == pointA && t.pointB == pointB && t.pointC==pointC;
		} else {
			return false;
		}
	}
	public int compareTo(TupleType t) {
		if (pointA != t.pointA) {
			return pointA < t.pointA ? -1 : 1;
		} else if (pointB != t.pointB) {
			return pointB < t.pointB ? -1 : 1;
		} else if(pointC != t.pointC){ 
			return pointC < t.pointC ? -1 : 1;
		}else{
			return 0;
		}
	}

	@Override
	public String toString() {
		return	"("+this.getA()+","+this.getB()+","+this.getC()+")";
	}
}
