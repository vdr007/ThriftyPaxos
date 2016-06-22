package glassPaxos.network;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.network.messages.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class NettyNetwork extends Network {
	private SimpleLogger LOG = SimpleLogger.getLogger("NettyNetwork");
	private boolean isRunning = true;
	private final LinkedBlockingQueue<Object> events = new LinkedBlockingQueue<Object>(2000);
	private final static PooledByteBufAllocator allocator = new PooledByteBufAllocator();
	private final Session[][] sessions;
	
	public NettyNetwork(NodeIdentifier myID, EventHandler eventHandler){
		super(myID, eventHandler);
		
		InetSocketAddress addr = Configuration.getNodeAddress(myID);
		setupServer(addr);
		new EventThread().start();
		new TimerInsertThread().start();
		sessions = new Session[NodeIdentifier.Role.values().length][];
		for(int i=0; i<sessions.length; i++){
			sessions[i] = new Session[Configuration.MAX_NODE_ID];
			for(int j=0; j<sessions[i].length; j++)
				sessions[i][j] = new Session();
		}
	}

	@Override
	public void localSendMessage(Message msg) {
	//public void localSendMessage(Object obj) {
		//System.out.printf("put msg into events\n");
		//Message m = (Message)msg;
		try{
			events.put(msg);
		}
		catch(InterruptedException e){
			LOG.warning(e);
		}
	}

	private void reportError(NodeIdentifier node, Throwable cause){
		try{
			events.put(new FailureEvent(node, cause));
		}
		catch(InterruptedException e){
			LOG.warning(e);
		}
	}
	
	private void setupServer(InetSocketAddress addr){
		EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ch.pipeline().addLast(new MessageDecoder(), new NettyInboundMessageHandler(null));
                 }
             })
             .option(ChannelOption.SO_BACKLOG, 128)         
             .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(addr).sync(); // (7)
            LOG.info("Server is binded to %s\n", addr);
        } catch (InterruptedException e) {
			LOG.warning(e);
		} finally {
            //workerGroup.shutdownGracefully();
            //bossGroup.shutdownGracefully();
        }
	}
	
	EventLoopGroup workerGroup = new NioEventLoopGroup();
	private Channel createConnection(InetSocketAddress addr, NodeIdentifier node) {
		

        try {
			final NodeIdentifier locNode = node;
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.option(ChannelOption.TCP_NODELAY, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new MessageEncoder(), new NettyInboundMessageHandler(locNode));
                }
            });
            
           

            // Start the client.
            LOG.info("Start to connect to %s\n", addr);
            b.connect(addr.getHostName(), addr.getPort());
            ChannelFuture f = b.connect(addr.getHostName(), addr.getPort()); // (5)
            boolean success = f.await(100);
            if(success && f.isSuccess()){
            	LOG.info("Connected to %s\n", addr);
            	return f.channel();
            }
            else{
            	LOG.warning("Connect to %s failed\n", addr);
            	reportError(node, f.cause());
            	return null;
            }
        } catch (InterruptedException e) {
			LOG.warning(e);
			return null;
		} finally {
            //workerGroup.shutdownGracefully();
        }
	}
	
	private final Object sendCompleteLock = new Object();
	private int outstandingWrites = 0;
	private final int outstandingWriteLimit = 10;
	private void increaseOutstanding(){
		synchronized(sendCompleteLock){
			outstandingWrites++;
		}
	}
	
	private void waitForOutstanding(){
		synchronized(sendCompleteLock){
			while(outstandingWrites >= outstandingWriteLimit){
				try{
					sendCompleteLock.wait();
				}
				catch(InterruptedException e){
					LOG.warning(e);
				}
			}
		}
	}
	
	private void decreaseOutstanding(){
		synchronized(sendCompleteLock){
			outstandingWrites --;
			if(outstandingWrites < outstandingWriteLimit){
				sendCompleteLock.notifyAll();
			}
		}
	}
	

	private static class Session {
		public NodeIdentifier receiver;
		public Channel channel;
		public ByteBuf bb;
		public SendCompleteHandler sendCompleteHandler;
	}
	
	private class SendCompleteHandler implements GenericFutureListener<ChannelFuture> {

		private NodeIdentifier node;
		
		public SendCompleteHandler(NodeIdentifier node){
			this.node = node;
		}
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			decreaseOutstanding();
			if(!future.isSuccess()){
				LOG.warning("Send to %s fails\n", node);
				LOG.warning(future.cause());
				Session s = sessions[node.getRole().ordinal()][node.getID()];
				synchronized(s){
					s.channel = null;
				}
				LOG.warning("Disconnected\n");
				reportError(node, future.cause());
			}
		}
		
	}

	@Override
	public void sendMessage(NodeIdentifier receiver, Message msg) {
		this.waitForOutstanding();
		final Session s = sessions[receiver.getRole().ordinal()][receiver.getID()];
		synchronized(s){
			if(s.channel == null){
				s.receiver = receiver;
				InetSocketAddress addr = Configuration.getNodeAddress(receiver);
				s.channel = createConnection(addr, receiver);
				if(s.channel == null)
					return;
				s.sendCompleteHandler = new SendCompleteHandler(receiver);
				//s.bb = allocator.buffer(Configuration.MAX_MSG_SIZE);
			}
			/*s.bb.retain();
			s.bb.clear();
			s.bb.writerIndex(4);
			msg.serialize(s.bb);
			int size = s.bb.writerIndex();
			s.bb.writerIndex(0);
			s.bb.writeInt(size - 4);
			s.bb.writerIndex(size);
			s.channel.writeAndFlush(s.bb).awaitUninterruptibly();*/
			this.increaseOutstanding();
			ChannelFuture f = s.channel.writeAndFlush(msg);
			f.addListener(s.sendCompleteHandler);
		}
		LOG.debug("%s sends %s to %s\n", myID, msg, receiver);
	}
	
	public class MessageEncoder extends MessageToByteEncoder<Message> {
        @Override
        public void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out)
                throws Exception {
        	int index = out.writerIndex();
            out.writeInt(0);
            msg.serialize(out);
			int size = out.writerIndex();
			out.writerIndex(index);
			out.writeInt(size - 4);
			out.writerIndex(index + size);
        }
    }
	
	private class MessageDecoder extends ByteToMessageDecoder { // (1)
	    @Override
	    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) { // (2)
	    	//System.out.println(in.capacity());  //vdr: 1024/512
			//System.out.println("readableBytes="+in.readableBytes());
			//in.markReaderIndex();
			//while(in.readableBytes() > 0)
			//	System.out.print(in.readByte()+" ");
			//System.out.println();
			//in.resetReaderIndex();
			if(in.readableBytes() < 4){
			//	System.out.println("<4 "+in.readableBytes());
				return;
			}
			int index = in.readerIndex();
			int tmp = in.readInt();
			//System.out.println("size="+tmp);
			if(tmp <= in.readableBytes()){
				Message m = Message.deserializeRaw(in);
				out.add(m);
				
			}
			else{
			//	System.out.println("reset readerIndex to "+index);
				in.readerIndex(index);
			}
			//System.out.println("end one decode");
	    }
	}
	
	private class NettyInboundMessageHandler extends ChannelInboundHandlerAdapter{
		
		private NodeIdentifier node;
		
		public NettyInboundMessageHandler(NodeIdentifier node){
			this.node = node;
		}
		
		@Override
	    public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
			Message m = (Message)msg;
			try{
				events.put(m);
			}
			catch(InterruptedException e){
				LOG.warning(e);
			}
	    }

	    @Override
	    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
	        // Close the connection when an exception is raised.
	        LOG.info(cause);
	        ctx.close();
	        if(node!=null)
	        	reportError(node, cause);
	    }
	}
	
	private static class TimerEvent {
	
	}
	private class TimerInsertThread extends Thread {
		public void run(){
			TimerEvent te = new TimerEvent();
			while(isRunning){
				try{
					Thread.sleep(100);
					events.put(te);
				}
				catch(InterruptedException e){
					LOG.warning(e);
				}
			}
		}
	}
	
	private class EventThread extends Thread {
		public void run(){
			while(isRunning){
				try{
					Object o = events.take();
					if(o instanceof TimerEvent){
						eventHandler.handleTimer();
					}
					else if(o instanceof Message){
						eventHandler.handleMessage((Message)o);
					}
					else if(o instanceof FailureEvent){
						FailureEvent f = (FailureEvent)o;
						eventHandler.handleFailure(f.node, f.cause);
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
	
	private static class FailureEvent {
		public NodeIdentifier node;
		public Throwable cause;
		
		public FailureEvent(NodeIdentifier node, Throwable cause){
			this.node = node;
			this.cause = cause;
		}
	}
}
