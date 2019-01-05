package req.Rand;

import org.apache.commons.math3.util.FastMath;

public class ExpGenerator implements RandomGenerator{
	RandomGenerator uniform;
	double lambda;
	int upper;

	public ExpGenerator(double lambda,int upper,RandomGenerator uniform){
		this.lambda=lambda;
		this.uniform=uniform;
		this.upper=upper;
	}

	@Override
	public int nextInt(){
		return (int)(nextDouble()*upper);
	}

	@Override
	public int nextInt(int upper){
		return (int)(nextDouble()*upper);
	}

	@Override
	public double nextDouble(){
		return -FastMath.log(1.0-uniform.nextDouble())/lambda;
	}
}
