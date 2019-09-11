package req.rand;

public interface RandomGenerator{
	int nextInt();
	int nextInt(int upper);
	double nextDouble();
	void setUpper(int upper);
}
