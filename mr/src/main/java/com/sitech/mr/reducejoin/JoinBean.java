package com.sitech.mr.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class JoinBean implements Writable{

	private long orderId;
	private String pid;
	private int amount;
	private String pname;
	
	
	
	public JoinBean() {
		super();
		// TODO Auto-generated constructor stub
	}
	

	public long getOrderId() {
		return orderId;
	}

	public void setOrderId(long orderId) {
		this.orderId = orderId;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(orderId);
		out.writeUTF(pid);
		out.writeInt(amount);
		out.writeUTF(pname);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.orderId=in.readLong();
		this.pid=in.readUTF();
		this.amount=in.readInt();
		this.pname=in.readUTF();
		}


	@Override
	public String toString() {
		return orderId + "\t" + pname + "\t" + amount;
	}
	
	

}
