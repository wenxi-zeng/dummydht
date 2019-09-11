package req.rand;

import util.Log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

public class ContentSink implements WritableByteChannel{
	static Log log=Log.get();
	static int buffer_size=1024*64;
	Log dynamicLog=log;
	static Charset utf8=Charset.forName("UTF-8");
	CharsetDecoder decoder=utf8.newDecoder();
	CharBuffer buffer;
	StringBuilder sb=new StringBuilder();
	boolean open=true;
	int truncate;
	boolean truncated=false;

	/**
	 * To digest file content.
	 *
	 * @param truncate print up to truncate chars to prevent flooding your log
	 */
	public ContentSink(int truncate){
		this.truncate=truncate;
		decoder.onMalformedInput(CodingErrorAction.REPLACE);
		decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
		buffer=CharBuffer.allocate(truncate);
	}

	/**
	 * To digest file content.
	 *
	 * @param l        if you would like to print to other Log than default one.
	 * @param truncate print up to truncate chars to prevent flooding your log
	 */
	public ContentSink(Log l,int truncate){
		this.truncate=truncate;
		decoder.onMalformedInput(CodingErrorAction.REPLACE);
		decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
		buffer=CharBuffer.allocate(truncate);
		this.dynamicLog=l;
	}

	public void flush(){
		sb.append("...");
		dynamicLog.i(sb.toString());
		sb.setLength(0);
	}

	public long transferFrom(ReadableByteChannel channel,long size) throws IOException{
		ByteBuffer bbuffer=ByteBuffer.allocate(buffer_size);
		long remain=size;
		while(bbuffer.remaining()<remain){
			remain-=channel.read(bbuffer);
			if(bbuffer.remaining()==0){
				bbuffer.flip();
				while(write(bbuffer)>0) ;
			}
		}
		bbuffer.limit(bbuffer.position()+(int)remain);
		while(remain>0){
			remain-=channel.read(bbuffer);
		}
		bbuffer.flip();
		while(bbuffer.hasRemaining()) write(bbuffer);
		return size;
	}

	protected int write(ByteBuffer src,boolean eof) throws IOException{
		if(truncated){
			src.position(src.limit());
		}
		int remain=src.remaining();
		CoderResult result=decoder.decode(src,buffer,eof);
		if(result==CoderResult.OVERFLOW){
			buffer.flip();
			sb.append(buffer);
			truncated=true;
			flush();
		}
		return remain-src.remaining();
	}

	@Override
	public int write(ByteBuffer src) throws IOException{
		return write(src,false);
	}

	@Override
	public boolean isOpen(){
		return open;
	}

	@Override
	public void close() throws IOException{
		open=false;
		flush();
	}
}
