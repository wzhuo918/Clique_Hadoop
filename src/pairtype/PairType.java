package pairtype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PairType implements WritableComparable<PairType> {
	protected long pointA = -1;
	protected long pointB = -1;

	public long getA() {
		return pointA;
	}

	public long getB() {
		return pointB;
	}
	public void setA(long a) {
		this.pointA=a;
	}

	public void setB(long b) {
		this.pointB=b;
	}
	public void set(long point1, long point2) {
		pointA = point1;
		pointB = point2;
	}
	public PairType(){}
	public PairType(long point1,long point2)
	{
		pointA=point1;
		pointB=point2;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(pointA);
		out.writeLong(pointB);
	}

	public void readFields(DataInput in) throws IOException {
		pointA = in.readLong();
		pointB = in.readLong();
	}

	public int hashCode() {
		return Integer.rotateLeft(String.valueOf(pointA * 13 + pointB)
				.hashCode(), 7);
	}
	public long getPointnotV(long v)
	{
		if(this.getA()!=v)
			return this.getA();
		else
			return this.getB();
	}

	@Override
	public boolean equals(Object pair) {
		if (pair instanceof PairType) {
			PairType p = (PairType) pair;
			return p.pointA == pointA && p.pointB == pointB;
		} else {
			return false;
		}
	}

	/** A Comparator that compares serialized PairType. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(PairType.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(PairType.class, new Comparator());
	}

	public int compareTo(PairType p) {
		if (pointA != p.pointA) {
			return pointA < p.pointA ? -1 : 1;
		} else if (pointB != p.pointB) {
			return pointB < p.pointB ? -1 : 1;
		} else {
			return 0;
		}
	}

	@Override
	public String toString() {
		return	this.getA()+","+this.getB();
	}
	
}
