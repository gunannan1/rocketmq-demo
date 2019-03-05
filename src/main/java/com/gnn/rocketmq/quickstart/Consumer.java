package com.gnn.rocketmq.quickstart;

import java.util.List;

import com.gnn.rocketmq.constants.Const;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 消费者
 */
public class Consumer {

	
	public static void main(String[] args) throws MQClientException {
		
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_quick_consumer_name");

		//单机
//		consumer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);

		//主从
		consumer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);

		//设置消费位置，这里选择从最后一个消息开始消费
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

		//订阅主题，"*"表示不区分tags，获取全部消息
		consumer.subscribe("test_quick_topic", "*");

		//监听topic
		consumer.registerMessageListener(new MessageListenerConcurrently() {

			/**
			 * 消费消息的方式
			 * @param msgs 内部对message的又一层封装
			 * @param context 全局对象
			 * @return
			 */
			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				//获取到MessageExt对象，由于之前发的是单条消息，所以用0
				MessageExt me = msgs.get(0);

				try {
					//获取消息里的各种属性
					String topic = me.getTopic();
					String tags = me.getTags();
					String keys = me.getKeys();

//					if(keys.equals("key1")) {
//						System.err.println("消息消费失败..");
//						int a = 1/0;
//					}

					String msgBody = new String(me.getBody(), RemotingHelper.DEFAULT_CHARSET);
					System.err.println("topic: " + topic + ",tags: " + tags + ", keys: " + keys + ",body: " + msgBody);
				} catch (Exception e) {
					e.printStackTrace();
					//消息重发了多少次
					int recousumeTimes = me.getReconsumeTimes();
					System.err.println("recousumeTimes: " + recousumeTimes);
					//最大错误为3后，就不处理了
					if(recousumeTimes == 3) {
						//		记录日志....
						//  	做补偿处理
						return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
					}

					//获取失败，重试
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		consumer.start();
		System.err.println("consumer start...");

	}
}
