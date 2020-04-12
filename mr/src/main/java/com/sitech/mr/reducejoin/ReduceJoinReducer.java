package com.sitech.mr.reducejoin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceJoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable>{

	private List<JoinBean> orderData = new ArrayList<JoinBean>();
	private Map<String, String> pData = new HashMap<String, String>();
	
	@Override
	protected void reduce(Text key, Iterable<JoinBean> value,
			Reducer<Text, JoinBean, JoinBean, NullWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		if (key.toString().contains("order")) {
			
			for(JoinBean joinBean : value) {
				JoinBean bean = new JoinBean();
				try {
					BeanUtils.copyProperties(bean, joinBean);
				} catch (IllegalAccessException | InvocationTargetException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				orderData.add(bean);
			}
			
		}else {
			for (JoinBean joinBean : value) {
				pData.put(joinBean.getPid(), joinBean.getPname());
				
			}
		}
	}

	@Override
	protected void cleanup(Reducer<Text, JoinBean, JoinBean, NullWritable>.Context context)
			throws IOException, InterruptedException {

		for (JoinBean joinBean : orderData) {
			joinBean.setPname(pData.get(joinBean.getPid()));
			
			context.write(joinBean, NullWritable.get());
		}
	}
	
	
}
