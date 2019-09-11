package req.rand;

import org.apache.commons.math3.util.FastMath;

/**
 * The code is copied from Apache common math experimental branch
 *
 * Wolfgang Hörmann and Gerhard Derflinger
 * "Rejection-inversion to generate variates from monotone discrete distributions."
 * ACM Transactions on Modeling and Computer Simulation (TOMACS) 6.3 (1996): 169-184.
 */
public class ZipfGenerator implements RandomGenerator{
	protected RandomGenerator gen;
	/**
	 * Exponent parameter of the distribution.
	 */
	double exponent;
	/**
	 * Number of elements.
	 */
	int upper;
	/**
	 * Constant equal to {@code hIntegral(1.5) - 1}.
	 */
	double hIntegralX1;
	/**
	 * Constant equal to {@code hIntegral(upper + 0.5)}.
	 */
	double hIntegralNumberOfElements;
	/**
	 * Constant equal to {@code 2 - hIntegralInverse(hIntegral(2.5) - h(2)}.
	 */
	double s;

	public ZipfGenerator(double alpha,int upper,RandomGenerator uniformGenerator){
		this.gen=uniformGenerator;
		this.upper=upper;
		this.exponent=alpha;
		this.hIntegralX1=hIntegral(1.5)-1d;
		this.hIntegralNumberOfElements=hIntegral(upper+0.5);
		this.s=2d-hIntegralInverse(hIntegral(2.5)-h(2));
	}

	@Override
	public double nextDouble(){
		return (double)nextInt()/upper;
	}

	@Override
	public int nextInt(int upper){
		if(upper!=this.upper){
			this.upper=upper;
			this.hIntegralNumberOfElements=hIntegral(upper+0.5);
		}
		return nextInt();
	}

	@Override
	public int nextInt(){
		while(true){
			final double u=hIntegralNumberOfElements+gen.nextDouble()*(hIntegralX1-hIntegralNumberOfElements);
			// u is uniformly distributed in (hIntegralX1, hIntegralNumberOfElements]

			double x=hIntegralInverse(u);
			int k=(int)(x+0.5);
			// Limit k to the range [1, numberOfElements]
			// (k could be outside due to numerical inaccuracies)
			if(k<1){
				k=1;
			}else if(k>upper){
				k=upper;
			}
			// Here, the distribution of k is given by:
			//
			//   P(k = 1) = C * (hIntegral(1.5) - hIntegralX1) = C
			//   P(k = m) = C * (hIntegral(m + 1/2) - hIntegral(m - 1/2)) for m >= 2
			//
			//   where C := 1 / (hIntegralNumberOfElements - hIntegralX1)

			if(k-x<=s || u>=hIntegral(k+0.5)-h(k)){

				// Case k = 1:
				//
				//   The right inequality is always true, because replacing k by 1 gives
				//   u >= hIntegral(1.5) - h(1) = hIntegralX1 and u is taken from
				//   (hIntegralX1, hIntegralNumberOfElements].
				//
				//   Therefore, the acceptance rate for k = 1 is P(accepted | k = 1) = 1
				//   and the probability that 1 is returned as random value is
				//   P(k = 1 and accepted) = P(accepted | k = 1) * P(k = 1) = C = C / 1^exponent
				//
				// Case k >= 2:
				//
				//   The left inequality (k - x <= s) is just a short cut
				//   to avoid the more expensive evaluation of the right inequality
				//   (u >= hIntegral(k + 0.5) - h(k)) in many cases.
				//
				//   If the left inequality is true, the right inequality is also true:
				//     Theorem 2 in the paper is valid for all positive exponents, because
				//     the requirements h'(x) = -exponent/x^(exponent + 1) < 0 and
				//     (-1/hInverse'(x))'' = (1+1/exponent) * x^(1/exponent-1) >= 0
				//     are both fulfilled.
				//     Therefore, f(x) := x - hIntegralInverse(hIntegral(x + 0.5) - h(x))
				//     is a non-decreasing function. If k - x <= s holds,
				//     k - x <= s + f(k) - f(2) is obviously also true which is equivalent to
				//     -x <= -hIntegralInverse(hIntegral(k + 0.5) - h(k)),
				//     -hIntegralInverse(u) <= -hIntegralInverse(hIntegral(k + 0.5) - h(k)),
				//     and finally u >= hIntegral(k + 0.5) - h(k).
				//
				//   Hence, the right inequality determines the acceptance rate:
				//   P(accepted | k = m) = h(m) / (hIntegrated(m+1/2) - hIntegrated(m-1/2))
				//   The probability that m is returned is given by
				//   P(k = m and accepted) = P(accepted | k = m) * P(k = m) = C * h(m) = C / m^exponent.
				//
				// In both cases the probabilities are proportional to the probability mass function
				// of the Zipf distribution.
				return k;
			}
		}
	}

	/**
	 * {@code H(x) :=}
	 * <ul>
	 * <li>{@code (x^(1-exponent) - 1)/(1 - exponent)}, if {@code exponent != 1}</li>
	 * <li>{@code log(x)}, if {@code exponent == 1}</li>
	 * </ul>
	 * H(x) is an integral function of h(x),
	 * the derivative of H(x) is h(x).
	 *
	 * @param x free parameter
	 * @return {@code H(x)}
	 */
	private double hIntegral(final double x){
		final double logX=FastMath.log(x);
		return helper2((1d-exponent)*logX)*logX;
	}

	/**
	 * {@code h(x) := 1/x^exponent}
	 *
	 * @param x free parameter
	 * @return h(x)
	 */
	private double h(final double x){
		return FastMath.exp(-exponent*FastMath.log(x));
	}

	/**
	 * The inverse function of H(x).
	 *
	 * @param x free parameter
	 * @return y for which {@code H(y) = x}
	 */
	private double hIntegralInverse(final double x){
		double t=x*(1d-exponent);
		if(t<-1d){
			// Limit value to the range [-1, +inf).
			// t could be smaller than -1 in some rare cases due to numerical errors.
			t=-1;
		}
		return FastMath.exp(helper1(t)*x);
	}

	/**
	 * Helper function that calculates {@code log(1+x)/x}.
	 * <p>
	 * A Taylor series expansion is used, if x is close to 0.
	 *
	 * @param x a value larger than or equal to -1
	 * @return {@code log(1+x)/x}
	 */
	static double helper1(final double x){
		if(FastMath.abs(x)>1e-8){
			return FastMath.log1p(x)/x;
		}else{
			return 1.-x*((1./2.)-x*((1./3.)-x*(1./4.)));
		}
	}

	/**
	 * Helper function to calculate {@code (exp(x)-1)/x}.
	 * <p>
	 * A Taylor series expansion is used, if x is close to 0.
	 *
	 * @param x free parameter
	 * @return {@code (exp(x)-1)/x} if x is non-zero, or 1 if x=0
	 */
	static double helper2(final double x){
		if(FastMath.abs(x)>1e-8){
			return FastMath.expm1(x)/x;
		}else{
			return 1.+x*(1./2.)*(1.+x*(1./3.)*(1.+x*(1./4.)));
		}
	}

	@Override
	public void setUpper(int upper) {
		this.upper = upper;
	}
}
