package glassPaxos.network;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.network.messages.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SimpleNetwork extends Network {

	private SimpleLogger LOG = SimpleLogger.getLogger("SimpleNetwork");
	private ServerSocket serverSocket = null;
	private HashMap<NodeIdentifier, Socket> outgoingSockets = new HashMap<NodeIdentifier, Socket>();
	private boolean isRunning = true;
	private LinkedBlockingQueue<Object> events = new LinkedBlockingQueue<Object>(1000);
	
	public SimpleNetwork(NodeIdentifier myID, EventHandler eventHandler){
		super(myID, eventHandler);
		
		InetSocketAddress addr = Configuration.getNodeAddress(myID);
		try {
			serverSocket = new ServerSocket();
			serverSocket.bind(addr);
			new ServerSocketThread().start();
			new EventThread().start();
			LOG.info("SimpleNetwork binds to %d\n", addr.getPort());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
		
	}

	PooledByteBufAllocator allocator = new PooledByteBufAllocator();
	ByteBuf bb = allocator.buffer(Configuration.MAX_MSG_SIZE);
	
	@Override
	public synchronized void sendMessage(NodeIdentifier receiver, Message msg) {
		Socket out = null;
		LOG.debug("%s sends %s to %s\n", myID, msg, receiver);
		try{
			if(outgoingSockets.containsKey(receiver)){
				out = outgoingSockets.get(receiver);
			}
			else{
				InetSocketAddress addr = Configuration.getNodeAddress(receiver);
				out = new Socket();
				out.connect(addr);
				out.setTcpNoDelay(true);
				outgoingSockets.put(receiver, out);
			}
		
			bb.clear();
			bb.writeInt(0);
			msg.serialize(bb);
			int size = bb.writerIndex() - 4;
			bb.writerIndex(0);
			bb.writeInt(size);
			byte[] tmp = new byte[size + 4];
			bb.getBytes(0, tmp);
			out.getOutputStream().write(tmp, 0, size + 4);
		}
		catch(IOException e){
			LOG.warning("Unable to send msg %s\n", msg);
			LOG.warning(e);
			outgoingSockets.remove(receiver);
		}
		
	}

	@Override
	public void localSendMessage(Message msg) {
	}

	private class ServerSocketThread extends Thread {
		public void run(){
			while(isRunning){
				try {
					Socket s = serverSocket.accept();
					new SocketThread(s).start();
				} catch (IOException e) {
					LOG.error(e);
					System.exit(-1);
				}
			}
		}
	}
	
	private class SocketThread extends Thread {
		private Socket s =  null;
		public SocketThread(Socket s){
			this.s = s;
		}
		
		public void run() {
			byte[] buf = new byte[Configuration.MAX_MSG_SIZE];
			ByteBuf bb = allocator.buffer(Configuration.MAX_MSG_SIZE);
			try {
				DataInputStream dis = new DataInputStream(s.getInputStream());
				while(isRunning && !s.isClosed()){
					int size = dis.readInt();
					dis.readFully(buf, 0, size);
					bb.writeBytes(buf, 0, size);
					Message msg = Message.deserializeRaw(bb);
					LOG.debug("%s receives %s from %s\n", myID, msg, msg.getSender());
					events.put(msg);
				}
			} catch (IOException e) {
				LOG.info(e);
			} catch (InterruptedException e){
				LOG.info(e);
			} finally {
				try{
					s.close();
				}
				catch(Exception e){}
			}
			
		}
	}
	
	private class EventThread extends Thread {
		public void run(){
			while(isRunning){
				try{
					Object o = events.poll(100, TimeUnit.MILLISECONDS);
					if(o == null){
						eventHandler.handleTimer();
					}
					else if(o instanceof Message){
						eventHandler.handleMessage((Message)o);
					}
				}
				catch(InterruptedException e){
					LOG.warning(e);
				}
			}
		}
	}
	
	public static void main(String []args) throws Exception{
		
	}
}
