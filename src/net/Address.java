package net;

import java.io.Serializable;

/**
 * IP:port address as key.
 */
public class Address implements Serializable{
	@Override
	public String toString() {
		return "Address [ip=" + ip + ", port=" + port + "]";
	}

	private final String ip;
	private final int port;

	public Address(String ip,int port){
		this.ip=ip;
		this.port=port;
	}

	public String getIp(){
		return ip;
	}

	public int getPort(){
		return port;
	}

	@Override
	public int hashCode(){
		return (ip+":"+port).hashCode();
	}

	@Override
	public boolean equals(Object obj){
		if(obj instanceof Address){
			Address address=(Address)obj;
			return this.ip.equals(address.ip) && this.port==address.port;
		}
		return false;
	}
}
