package req;

import req.rand.RandomGenerator;
import req.rand.UniformGenerator;
import util.Log;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StaticTree{
    static Log log=Log.get();
    RandomGenerator generator;
    List<RandTreeNode> emptyDirs=new ArrayList<>();
    List<RandTreeNode> nonEmptyDirs=new ArrayList<>();  //  it's pointless to ls empty dirs
    List<RandTreeNode> files=new ArrayList<>();
    String sep=null;

    public static Log getLog() {
        return log;
    }

    public RandomGenerator getGenerator() {
        return generator;
    }

    public List<RandTreeNode> getEmptyDirs() {
        return emptyDirs;
    }

    public List<RandTreeNode> getNonEmptyDirs() {
        return nonEmptyDirs;
    }

    public List<RandTreeNode> getFiles() {
        return files;
    }

    public String getSep() {
        return sep;
    }

    public void shuffleFiles(String file) throws IOException{
        List<Integer> list=parseShuffle(file);
        if(list.size()!=files.size())
            throw new IllegalArgumentException("Internal file size: "+files.size()+" shuffle size: "+list.size());
        for(int i=0;i<files.size();++i){
            RandTreeNode n1=files.get(i);
            int j=i;
            for(;;){
                int k=list.get(j);
                list.set(j,j);
                if(k==i) break;
                files.set(j,files.get(k));
                j=k;
            }
            files.set(j,n1);
        }
    }

    public void shuffleFiles(){
        plainShuffle(files,generator);
    }

    public void shuffleFilesUneven(String shuffle) throws IOException{
        try(BufferedReader in=new BufferedReader(new InputStreamReader(
                new FileInputStream(shuffle),"UTF-8"))){
            List<List<Integer>> order=new ArrayList<>();
            for(String line;(line=in.readLine())!=null;){
                String[] strArray=line.split(",\\s");
                List<Integer> sort=new ArrayList<>();
                for(String s : strArray){
                    if(s.trim().length()>0) sort.add(Integer.parseInt(s));
                }
                order.add(sort);
            }
            unevenShuffle(files,generator,order);
        }
    }

    //  Fisher–Yates
    public static <T> void plainShuffle(List<T> list,RandomGenerator generator){
        for(int i=list.size()-1;i>0;--i){
            int j=generator.nextInt(i+1);
            T t=list.get(i);
            list.set(i,list.get(j));
            list.set(j,t);
        }
    }

//	static <T,K> void evenShuffle(List<T> list,RandomGenerator generator,List<List<K>> weight){
//		if(list.size()!=weight.size())
//			throw new IllegalArgumentException("Original list size and weight size do not match.");
//		Map<K, Integer> counter=new HashMap<>();
//		for(List<K> entry : weight){
//			for(K key : entry){
//				Integer i=counter.get(key);
//				counter.put(key,i==null ? 1 : (i+1));
//			}
//		}
//		int thresh=Collections.min(counter.values());
//		int[] c=new int[counter.size()];
//		Arrays.fill(c,0);
//
//		Map<K, List<Integer>> map=getCounter(weight);
//		skew(list,generator,map);
//	}
//
//	private static <K> Map<K, List<Integer>> getCounter(List<List<K>> weight){
//		Map<K, List<Integer>> map=new HashMap<>();
//		for(int i=0;i<weight.size();++i){
//			List<K> lsk=weight.get(i);
//			for(K key : lsk){
//				List<Integer> counter=map.get(key);
//				if(counter==null){
//					counter=new ArrayList<>();
//					map.put(key,counter);
//				}
//				counter.add(i);
//			}
//		}
//		return map;
//	}
//
//	private static <K,V> Map.Entry<K, V> getRandEntry(Map<K, V> map){
//		for(Map.Entry<K, V> entry : map.entrySet()) return entry;
//		return null;
//	}
//
//	private static <T,K> void skew(List<T> list,RandomGenerator generator,Map<K, List<Integer>> map){
//		for(List<Integer> entry : map.values()){
//			for(int k=entry.size()-1;k>=0;--k){
//				int i=entry.get(k);
//				int j=generator.nextInt(i+1);
//				T t=list.get(i);
//				list.set(i,list.get(j));
//				list.set(j,t);
//			}
//		}
//	}

    public static <T,K> void unevenShuffle(List<T> list,RandomGenerator generator,List<List<K>> weight){
        if(list.size()!=weight.size())
            throw new IllegalArgumentException("Original list size and weight size do not match.");
//		Map<K, List<Integer>> map=getCounter(weight);
        Set<K> set=new HashSet<>();
        for(int i=Math.min(1024,weight.size()-1);i>=0;--i){
            for(K key : weight.get(i)) set.add(key);
        }
        List<K> l=new ArrayList<>(set);
        K chosen=l.get(generator.nextInt(l.size()));
        List<T> removeList=new ArrayList<>();
        List<T> keepList=new ArrayList<>();
        for(int i=0;i<weight.size();++i){
            if(weight.get(i).contains(chosen)) removeList.add(list.get(i));
            else keepList.add(list.get(i));
        }
        list.clear();
        plainShuffle(keepList,generator);
        plainShuffle(removeList,generator);
        list.addAll(removeList);
        list.addAll(keepList);
    }

    static public List<Integer> parseShuffle(String file) throws IOException{
        try(BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(file),"UTF8"))){
            List<Integer> result=new ArrayList<>();
            for(String line;(line=br.readLine())!=null;){
                if(line.trim().length()==0) continue;
                try{
                    result.add(Integer.parseInt(line));
                }catch(NumberFormatException e){
                    log.w(e);
                }
            }
            return result;
        }
    }

    protected RandTreeNode emptyNode(){
        return new RandTreeNode();
    }

    protected StaticTree(){
        this(new UniformGenerator());
    }

    protected StaticTree(RandomGenerator generator){
        this.generator=generator;
    }

    public StaticTree(RandomGenerator generator,String sep){
        this.generator=generator;
        this.sep=sep;
    }

    public static StaticTree getStaticTree(String filename) throws IOException{
        StaticTree tree=new StaticTree();
        new TreeParser<StaticTree, RandTreeNode>().parse(tree,filename,false);
        return tree;
    }

    protected String randName(){
        return String.format("%8X%8X",generator.nextInt(),generator.nextInt());
    }

    public class RandTreeNode{
        RandTreeNode parent=null;
        String name=null;
        long size=0;

        protected RandTreeNode(){
        }

        protected RandTreeNode(RandTreeNode parent,String name,long size){
            this.parent=parent;
            this.name=name;
            this.size=size;
        }

        protected void setParent(RandTreeNode p){
            parent=p;
        }

        protected RandTreeNode(RandTreeNode parent,String name){
            this.parent=parent;
            this.name=name;
        }

        public RandTreeNode getParent() {
            return parent;
        }

        public String getName() {
            return name;
        }

        public long getSize() {
            return size;
        }

        @Override
        public String toString(){
            if(parent!=null){
                if(parent.name.endsWith(sep))
                    return parent.toString()+name;
                else return parent.toString()+sep+name;
            }else return name;
        }
    }

    public int getNonEmptyDirSize(){
        return nonEmptyDirs.size();
    }

    public int getFileSize(){
        return files.size();
    }

    public void updateFileSize(int index,long newSize){
        files.get(index).size=newSize;
    }

    protected static class TreeParser<Tree extends StaticTree,Node extends StaticTree.RandTreeNode>{
        static Pattern reg=Pattern.compile("(\\[\\s*(\\d+)\\s+(\\d+)\\]\\s+)?(.+)");
        Deque<Node> ancestors=new ArrayDeque<>();
        Deque<Node> pendingFiles=new ArrayDeque<>();

        protected void rollback(int indent,Tree tree,boolean fillEmpty){
            if(ancestors.size()>0){
                Node last=ancestors.removeLast();
                if(ancestors.size()==0){
                    if(fillEmpty) tree.emptyDirs.add(last);
                }else{
                    if(tree.sep==null){
                        pendingFiles.add(last);
                    }//else{
//						if(last.name.endsWith(tree.sep)){    //  dir
//							if(fillEmpty) tree.emptyDirs.add(last);  //  empty dir
//						}else tree.files.add(last); //  file
//					}
                    while(ancestors.size()>indent) ancestors.removeLast();
                }
            }
        }

        protected void vacuum(Tree tree,boolean fillEmpty){
            Node node;
            while((node=pendingFiles.pollFirst())!=null){
                if(!node.name.endsWith(tree.sep)) tree.files.add(node);
                else if(fillEmpty) tree.emptyDirs.add(node);
            }
        }

        protected void parse(Tree tree,String filename,RandomGenerator generator,boolean fillEmpty) throws IOException{
            tree.generator=generator;
            parse(tree,filename,fillEmpty);
        }

        protected void parse(Tree tree,String filename,boolean fillEmpty) throws IOException{
            long count=1;
            try(BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(filename),"UTF8"))){
                for(String line;(line=br.readLine())!=null;++count){
                    //  skip blank line
                    if(line.trim().length()==0) continue;
                    int indent=0;
                    String name=line;
                    while(name.startsWith("└── ") || name.startsWith("├── ") || name.startsWith("│   ") || name.startsWith("    ")){
                        name=name.substring(4);
                        ++indent;
                    }

                    name=name.trim();
//					System.out.println("path:"+name+":"+indent);
                    Node node=(Node)tree.emptyNode();
                    //  set name and possibly size
                    if(indent==0 && name.startsWith("directory")){  //  root
                        name=name.replaceFirst("directory\\s*","");
//						System.out.println("Root:"+name);
                    }else if(indent>0){   //  non root, check [size,modify time] part
                        Matcher m=reg.matcher(name);
                        if(m.matches()){
//							System.out.println("[0]"+m.group(0)+" [1]"+m.group(1)+" [2]"+m.group(2)+" [3]"+m.group(3)+" [4]"+m.group(4));
                            name=m.group(4);
                            if(m.group(1)!=null){
                                node.size=Long.parseLong(m.group(2));
                                //  don't care about modify time
                            }else{
                                // pattern=false;
                                continue;
                            }
                        }else throw new InvalidParameterException(String.format("%d: %s",count,line));
                    }
                    node.name=name;
                    //  cannot find direct parent
                    if(ancestors.size()<indent) throw new InvalidParameterException(String.format("%d: %s",count,line));
                    //  push to lists
                    boolean firstChild=(ancestors.size()==indent);
                    if(!firstChild){
                        rollback(indent,tree,fillEmpty);
                    }
                    if(indent>0){
                        Node parent=ancestors.getLast();
                        node.setParent(parent);
                        if(firstChild){
                            tree.nonEmptyDirs.add(parent);
                            if(tree.sep==null) pendingFiles.remove(parent);
                            if(tree.sep==null && indent>1){
                                tree.sep=parent.name.substring(parent.name.length()-1);
                                vacuum(tree,fillEmpty);
                            }
                        }
                    }
                    if(tree.sep!=null && !name.endsWith(tree.sep))
                        tree.files.add(node);
                    ancestors.add(node);
                }
                rollback(0,tree,fillEmpty);
                vacuum(tree,fillEmpty);
            }
        }
    }
}
