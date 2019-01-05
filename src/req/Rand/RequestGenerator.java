package req.Rand;

import req.Request;

import java.util.*;

public class RequestGenerator{
	Request.ReqType[] types;
	int len;
	RandomGenerator gen;
	double[] steps;

	static class ValueComparator implements Comparator<Map.Entry<Request.ReqType, Double>>{
		@Override
		public int compare(Map.Entry<Request.ReqType, Double> o1,Map.Entry<Request.ReqType, Double> o2){
			double neg=o2.getValue()-o1.getValue();
			return neg>0 ? 1 : neg<0 ? -1 : 0;
		}
	}

	public RequestGenerator(Map<Request.ReqType, Double> ratio,RandomGenerator uniform){
		gen=uniform;
		len=ratio.size();
		types=new Request.ReqType[len];
		steps=new double[len];
		List<Map.Entry<Request.ReqType, Double>> sorted=new ArrayList<>();
		sorted.addAll(ratio.entrySet());
		Collections.sort(sorted,new ValueComparator());
		double sum=0.0;
		int i=0;
		for(Map.Entry<Request.ReqType, Double> entry : sorted){
			types[i]=entry.getKey();
			steps[i++]=entry.getValue();
//			System.out.println(entry.getKey()+":"+entry.getValue());
			sum+=entry.getValue();
		}
		if(len>0){
			steps[0]=steps[0]/sum;
			for(i=1;i<len;++i){
				steps[i]=steps[i]/sum+steps[i-1];
			}
		}
	}

	public Request.ReqType next(){
		double rd=gen.nextDouble();
		for(int i=0;i<len;++i){
			if(rd<steps[i]) return types[i];
		}
		return null;
	}
}
