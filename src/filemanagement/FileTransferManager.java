package filemanagement;

import commonmodels.PhysicalNode;
import util.SimpleLog;

import java.util.ArrayList;
import java.util.List;

import static util.Config.NUMBER_OF_HASH_SLOTS;

public class FileTransferManager {

    private LocalFileManager localFileManager;

    private static volatile FileTransferManager instance = null;

    private List<FileTransferRequestCallBack> callBacks;

    private FileTransferManager() {
        localFileManager = LocalFileManager.getInstance();
        callBacks = new ArrayList<>();
    }

    public static FileTransferManager getInstance() {
        if (instance == null) {
            synchronized(FileTransferManager.class) {
                if (instance == null) {
                    instance = new FileTransferManager();
                }
            }
        }

        return instance;
    }

    public void subscribe(FileTransferRequestCallBack callBack) {
        callBacks.add(callBack);
    }

    public void unsubscribe(FileTransferRequestCallBack callBack) {
        callBacks.remove(callBack);
    }

    public void transfer(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        int numberOfFilesTransferred = 0;
        long sizeOfFilesTransferred = 0;

        for (int bucket : buckets ){
            FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
            if (fileBucket == null) continue;

            numberOfFilesTransferred += fileBucket.getNumberOfFiles();
            sizeOfFilesTransferred += fileBucket.getSize();
        }

        SimpleLog.i("Message from : " + from.getId() + ": Transfer to " + toNode.getId() + " completed. Number of files transferred: " + numberOfFilesTransferred + ", Total size: " + sizeOfFilesTransferred);
    }

    public void copy(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        int numberOfFilesReplicted = 0;
        long sizeOfFilesReplicated = 0;

        for (int bucket : buckets ){
            FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
            if (fileBucket == null) continue;

            numberOfFilesReplicted += fileBucket.getNumberOfFiles();
            sizeOfFilesReplicated += fileBucket.getSize();
        }

        SimpleLog.i("Message from : " + from.getId() + ": Replicate files to " + toNode.getId() + "Transfer completed. Number of files transferred: " + numberOfFilesReplicted + ", Total size: " + sizeOfFilesReplicated);
    }

    public void requestTransfer(int hi, int hf, PhysicalNode from, PhysicalNode toNode) {
        callFileTransfer(rangeToList(hi, hf), from ,toNode);
    }

    public void requestTransfer(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        callFileTransfer(buckets, from ,toNode);
    }

    public void requestTransfer(int bucket, PhysicalNode from, PhysicalNode toNode) {
        List<Integer> buckets = new ArrayList<>();
        buckets.add(bucket);
        callFileTransfer(buckets, from ,toNode);
    }

    public void requestCopy(int hi, int hf, PhysicalNode from, PhysicalNode toNode) {
        callFileReplicate(rangeToList(hi, hf), from ,toNode);
    }

    public void requestCopy(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        callFileReplicate(buckets, from ,toNode);
     }

    public void requestCopy(int bucket, PhysicalNode from, PhysicalNode toNode) {
        List<Integer> buckets = new ArrayList<>();
        buckets.add(bucket);
        callFileReplicate(buckets, from ,toNode);
    }

    private List<Integer> rangeToList(int hi, int hf) {
        List<Integer> buckets = new ArrayList<>();

        if (hf < hi) {
            for (int bucket = hi + 1; bucket <= NUMBER_OF_HASH_SLOTS; bucket++) {
                buckets.add(bucket);
            }
            for (int bucket = 0; bucket <= hf; bucket++) {
                buckets.add(bucket);
            }
        }
        else {
            for (int bucket = hi + 1; bucket <= hf; bucket++) {
                buckets.add(bucket);
            }
        }

        return buckets;
    }

    private void callFileTransfer(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        if (callBacks != null)
            for (FileTransferRequestCallBack callBack : callBacks) {
                callBack.onFileTransfer(buckets, from , toNode);
            }
    }

    private void callFileReplicate(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        if (callBacks != null)
            for (FileTransferRequestCallBack callBack : callBacks) {
                callBack.onFileReplicate(buckets, from , toNode);
            }
    }

    public interface FileTransferRequestCallBack {
        void onFileTransfer(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode);
        void onFileReplicate(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode);
    }
}
