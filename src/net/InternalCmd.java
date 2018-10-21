package net;

/**
 * Created by Yongtao on 8/28/2015.
 *
 * Internal command to communicate within IOControl
 * Add your control code into IOControl$Coordinator and more command types here
 */
public class InternalCmd{
	@Override
	public String toString() {
		return "InternalCmd [attachment=" + attachment + ", cmd=" + cmd
				+ ", getCMD()=" + getCMD() + ", getAttachment()="
				+ getAttachment() + ", getClass()=" + getClass()
				+ ", hashCode()=" + hashCode() + ", toString()="
				+ super.toString() + "]";
	}

	private Object attachment=null;
	private CMD cmd=null;

	public InternalCmd(){
	}

	public InternalCmd(CMD cmd){
		this.cmd=cmd;
	}

	public InternalCmd(CMD cmd,Object obj){
		this.cmd=cmd;
		this.attachment=obj;
	}

	public CMD getCMD(){
		return this.cmd;
	}

	public void setCMD(CMD cmd){
		this.cmd=cmd;
	}

	public Object getAttachment(){
		return this.attachment;
	}

	public void setAttachment(Object obj){
		this.attachment=obj;
	}

	public enum CMD{
		EXIT,OK
	}
}
