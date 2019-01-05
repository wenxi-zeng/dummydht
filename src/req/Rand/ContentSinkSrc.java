package req.Rand;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class ContentSinkSrc extends FileChannel{
	ContentSink sink=null;
	ContentSrc src=null;

	public ContentSinkSrc(int truncate){
		sink=new ContentSink(truncate);
	}

	public ContentSinkSrc(String path){
		src=new ContentSrc(path);
	}

	public ContentSinkSrc(String path,int truncate){
		src=new ContentSrc(path);
		sink=new ContentSink(truncate);
	}

	@Override
	public int read(ByteBuffer dst) throws IOException{
		if(src==null)
			throw new UnsupportedOperationException("ContentSinkSrc(String path,int truncate) should be called to have ContentSrc");
		return src.read(dst);
	}

	@Override
	public long read(ByteBuffer[] dsts,int offset,int length) throws IOException{
		throw new UnsupportedOperationException();
	}

	@Override
	public int write(ByteBuffer src) throws IOException{
		if(sink==null)
			throw new UnsupportedOperationException("ContentSinkSrc(String path,int truncate) should be called to have ContentSink");
		return sink.write(src);
	}

	@Override
	public long write(ByteBuffer[] srcs,int offset,int length) throws IOException{
		throw new UnsupportedOperationException();
	}

	@Override
	public long position() throws IOException{
		throw new UnsupportedOperationException();
	}

	@Override
	public FileChannel position(long newPosition) throws IOException{
		throw new UnsupportedOperationException();
	}

	@Override
	public long size() throws IOException{
		throw new UnsupportedOperationException();
	}

	@Override
	public FileChannel truncate(long size) throws IOException{
		throw new UnsupportedOperationException();
	}

	@Override
	public void force(boolean metaData) throws IOException{
		throw new UnsupportedOperationException();
	}

	@Override
	public long transferTo(long position,long count,WritableByteChannel target) throws IOException{
		if(src==null)
			throw new UnsupportedOperationException("ContentSinkSrc(String path,int truncate) should be called to have ContentSrc");
		return src.transferTo(target,count);
	}

	@Override
	public long transferFrom(ReadableByteChannel src,long position,long count) throws IOException{
		if(sink==null)
			throw new UnsupportedOperationException("ContentSinkSrc(String path,int truncate) should be called to have ContentSink");
		return sink.transferFrom(src,count);
	}

	@Override
	public int read(ByteBuffer dst,long position) throws IOException{
		return read(dst);
	}

	@Override
	public int write(ByteBuffer src,long position) throws IOException{
		return write(src);
	}

	@Override
	public MappedByteBuffer map(MapMode mode,long position,long size) throws IOException{
		throw new UnsupportedOperationException();
	}

	@Override
	public FileLock lock(long position,long size,boolean shared) throws IOException{
		throw new UnsupportedOperationException();
	}

	@Override
	public FileLock tryLock(long position,long size,boolean shared) throws IOException{
		throw new UnsupportedOperationException();
	}

	@Override
	protected void implCloseChannel() throws IOException{

	}
}
