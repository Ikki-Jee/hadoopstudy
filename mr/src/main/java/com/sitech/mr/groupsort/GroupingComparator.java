package com.sitech.mr.groupsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator{

	

	public GroupingComparator() {
		super(OrderBean.class, true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		OrderBean aBean = (OrderBean)a;
		OrderBean bBean = (OrderBean)b;

		
		return aBean.getOrder_id()>bBean.getOrder_id()?1:
			(aBean.getOrder_id()<bBean.getOrder_id()?-1:0);
	}

	
}
