package req.rand;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

public class ContentSrc implements ReadableByteChannel{
	static Charset utf8=Charset.forName("UTF-8");
	ByteBuffer buffer;
	boolean open=true;

	public ContentSrc(String path){
		buffer=ByteBuffer.wrap((path+';').getBytes(utf8)).asReadOnlyBuffer();
	}

	@Override
	public int read(ByteBuffer dst) throws IOException{
		int count=0;
		while(dst.hasRemaining()){
			int remain=buffer.remaining();
			if(remain==0){
				buffer.rewind();
				remain=buffer.remaining();
			}
			dst.put(buffer);
			count+=(remain-buffer.remaining());
		}
		return count;
	}

	public long transferTo(WritableByteChannel channel,long count) throws IOException{
		long remain=count;
		while(buffer.remaining()<=remain){
			remain-=channel.write(buffer);
			if(buffer.remaining()==0) buffer.rewind();
		}
		if(remain>0){
			int old_limit=buffer.limit();
			buffer.limit(buffer.position()+(int)remain);
			remain-=channel.write(buffer);
			buffer.limit(old_limit);
		}
		return count-remain;
	}

	@Override
	public boolean isOpen(){
		return open;
	}

	@Override
	public void close(){
		open=false;
	}
}
