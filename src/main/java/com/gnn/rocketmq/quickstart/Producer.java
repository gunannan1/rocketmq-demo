package com.gnn.rocketmq.quickstart;

import java.util.List;

import com.gnn.rocketmq.constants.Const;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;


/**
 * 生产者
 */
public class Producer {

	public static void main(String[] args) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		
		DefaultMQProducer producer = new DefaultMQProducer("test_quick_producer_name");

		//单机
//		producer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);

//		//主从
		producer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);


		
		producer.start();

		
		for(int i = 0 ; i <5; i ++) {
			//	1.	创建消息
			//topic为主题，生成者往主题发消息，消费者向主题订阅
			//tags 标签，做消息过滤
			//keys用户自定义的key ,一般用作唯一的标识
			//body是消息实体，这个实体是byte字节数组
			Message message = new Message("test_quick_topic",
					"TagA",
					"key" + i,
					("Hello RocketMQ" + i).getBytes());

//			SendResult sr=producer.send(message);
//			System.err.println("消息发出："+sr);


			//	2.1	同步发送消息
//			if(i == 1) {
//				message.setDelayTimeLevel(3);
//			}

//			SendResult sr = producer.send(message, new MessageQueueSelector() {
//
//				@Override
//				public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
//					Integer queueNumber = (Integer)arg;
//					return mqs.get(queueNumber);
//				}
//			}, 2);
//			System.err.println(sr);

//			SendResult sr = producer.send(message);
//			SendStatus status = sr.getSendStatus();
//			System.err.println(status);



			//System.err.println("消息发出: " + sr);

			//  2.2 异步发送消息
			producer.send(message, new SendCallback() {
				//rabbitmq急速入门的实战: 可靠性消息投递
				@Override
				public void onSuccess(SendResult sendResult) {
					System.err.println("msgId: " + sendResult.getMsgId() + ", status: " + sendResult.getSendStatus());
				}
				@Override
				public void onException(Throwable e) {
					e.printStackTrace();
					System.err.println("------发送失败");
				}
			});
		}

		//消息发完后关闭。
		//异步发送时要把这里注释掉，不然会直接把producer发掉
//		producer.shutdown();
		
	}
}
