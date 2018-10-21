package net;

import util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Yongtao on 9/17/2015.
 *
 * A simple log server to print received log to console,
 * if you need to do more, modify code in Worker.
 */
public class LogServer{
	private static final ExecutorService pool=Executors.newCachedThreadPool();
	private static final Log log=Log.get();

	public LogServer(int port) throws IOException{
		ServerSocketChannel server=ServerSocketChannel.open();
		Selector selector=Selector.open();
		server.socket().bind(new InetSocketAddress(port));
		server.configureBlocking(false);
		server.register(selector,SelectionKey.OP_ACCEPT);
		Coordinator coordinator=new Coordinator(selector);
		pool.execute(coordinator);
	}

	class Coordinator implements Runnable{
		private Selector selector;

		Coordinator(Selector selector){
			this.selector=selector;
		}

		@Override
		public void run(){
			for(;;){
				int count;
				try{
					count=selector.select();
				}catch(IOException e){
					log.w(e);
					break;
				}
				if(count!=0){
					Iterator<SelectionKey> it=selector.selectedKeys().iterator();
					while(it.hasNext()){
						SelectionKey key=it.next();
						it.remove();
						ServerSocketChannel ssc=(ServerSocketChannel)key.channel();
						SocketChannel channel;
						try{
							channel=ssc.accept();
						}catch(IOException ignored){
							continue;
						}
						Worker worker=new Worker(channel);
						pool.execute(worker);
					}
				}
			}
		}
	}

	class Worker implements Runnable{
		private SocketChannel socketChannel;

		Worker(SocketChannel socketChannel){
			this.socketChannel=socketChannel;
		}

		@Override
		public void run(){
			Socket socket=socketChannel.socket();
			try{
				InputStream is=socket.getInputStream();
				BufferedReader br=new BufferedReader(new InputStreamReader(is));
				String line;
				String ip=socket.getInetAddress().getHostAddress();
				int port=socket.getPort();
				String prefix="["+ip+":"+port+"]";
				while((line=br.readLine())!=null){
					System.out.println(prefix+" "+line);
					// todo save to log file, etc
					
					
				}
			}catch(IOException ignored){
			}finally{
				try{
					socket.close();
				}catch(IOException ignored){
				}
			}
		}
	}
}
