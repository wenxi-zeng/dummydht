package req.Rand;

import java.util.concurrent.ThreadLocalRandom;

public class UniformGenerator implements RandomGenerator{
	protected int upper;

	public UniformGenerator(int upper){
		this.upper=upper;
	}

	public UniformGenerator(){
		this.upper=Integer.MAX_VALUE;
	}

	@Override
	public int nextInt(){
		return nextInt(this.upper);
	}

	@Override
	public int nextInt(int upper){
		return ThreadLocalRandom.current().nextInt(upper);
	}

	@Override
	public double nextDouble(){
		return ThreadLocalRandom.current().nextDouble();
	}

	//  http://stackoverflow.com/a/2546186/5573989
	public long nextLong(long upper){
		// error checking and 2^x checking removed for simplicity.
		long bits, val;
		do{
			bits=(ThreadLocalRandom.current().nextLong()<<1)>>>1;
			val=bits%upper;
		}while(bits-val+(upper-1)<0L);
		return val;
	}
}
