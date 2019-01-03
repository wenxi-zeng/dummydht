package filemanagement;

import commonmodels.PhysicalNode;
import util.Config;
import util.SimpleLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

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

    public String transfer(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        int numberOfFilesTransferred = 0;
        long sizeOfFilesTransferred = 0;
        List<FileBucket> fileBuckets = new ArrayList<>();

        for (int bucket : buckets ){
            FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
            if (fileBucket == null) continue;

            fileBucket.setLocked(true);
            fileBuckets.add(fileBucket);
            numberOfFilesTransferred += fileBucket.getNumberOfFiles();
            sizeOfFilesTransferred += fileBucket.getSize();
        }

        float transferTime = (sizeOfFilesTransferred / Config.getInstance().getNetworkSpeed()) * 1000;

        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                callTransmitted(fileBuckets, from, toNode);
                cleanBuckets(fileBuckets);
            }
        }, (long)transferTime);


        String result = "Message from : " + from.getId()
                + ":\n         Transferring to " + toNode.getId()
                + ".\n         Number of files to be transferred: " + numberOfFilesTransferred
                + ",\n         Total size: " + sizeOfFilesTransferred
                + ",\n         Estimated time: " + transferTime;
        SimpleLog.i(result);

        return result;
    }

    public String copy(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        int numberOfFilesReplicated = 0;
        long sizeOfFilesReplicated = 0;
        List<FileBucket> fileBuckets = new ArrayList<>();

        for (int bucket : buckets ){
            FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
            if (fileBucket == null) continue;

            fileBucket.setLocked(true);
            fileBuckets.add(fileBucket);
            numberOfFilesReplicated += fileBucket.getNumberOfFiles();
            sizeOfFilesReplicated += fileBucket.getSize();
        }

        float replicateTime = (sizeOfFilesReplicated / Config.getInstance().getNetworkSpeed()) * 1000;

        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                callTransmitted(fileBuckets, from, toNode);
                unlockBucket(fileBuckets);
            }
        }, (long)replicateTime);

        String result = "Message from : " + from.getId()
                + ":\n         Transferring to " + toNode.getId()
                + ".\n         Number of files to be replicated: " + numberOfFilesReplicated
                + ",\n         Total size: " + sizeOfFilesReplicated
                + ",\n         Estimated time: " + replicateTime;
        SimpleLog.i(result);

        return result;
    }

    public String received(List<FileBucket> buckets, PhysicalNode from, PhysicalNode toNode) {
        int numberOfFilesReceived = 0;
        long sizeOfFilesReceived = 0;

        for (FileBucket bucket : buckets ){
            FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket.getKey());
            if (fileBucket == null) {
                fileBucket = new FileBucket(bucket.getKey());
                localFileManager.getLocalBuckets().put(fileBucket.getKey(), fileBucket);
            }

            fileBucket.setLocked(false);
            fileBucket.setNumberOfFiles(fileBucket.getNumberOfFiles() +  bucket.getNumberOfFiles());
            fileBucket.setSize(fileBucket.getSize() + bucket.getSize());

            numberOfFilesReceived += bucket.getNumberOfFiles();
            sizeOfFilesReceived += bucket.getSize();
        }

        float totalTime = (sizeOfFilesReceived / Config.getInstance().getNetworkSpeed()) * 1000;
        String result = "Message from : " + toNode.getId()
                + ":\n         File transmitted from " + from.getId()
                + ".\n         Number of files received: " + numberOfFilesReceived
                + ",\n         Total size: " + sizeOfFilesReceived
                + ",\n         Estimated time: " + totalTime;
        SimpleLog.i(result);

        return result;
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
            for (int bucket = hi + 1; bucket <= Config.getInstance().getNumberOfHashSlots(); bucket++) {
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
                callBack.onTransferring(buckets, from , toNode);
            }
    }

    private void callFileReplicate(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        if (callBacks != null)
            for (FileTransferRequestCallBack callBack : callBacks) {
                callBack.onReplicating(buckets, from , toNode);
            }
    }

    private void callTransmitted(List<FileBucket> buckets, PhysicalNode from, PhysicalNode toNode) {
        if (callBacks != null)
            for (FileTransferRequestCallBack callBack : callBacks) {
                callBack.onTransmitted(buckets, from, toNode);
            }
    }

    private void cleanBuckets(List<FileBucket> buckets) {
        for (FileBucket bucket : buckets ){
            localFileManager.getLocalBuckets().remove(bucket.getKey());
        }
    }

    private void unlockBucket(List<FileBucket> buckets) {
        for (FileBucket bucket : buckets ){
            FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket.getKey());
            if (fileBucket == null) continue;

            fileBucket.setLocked(false);
        }
    }

    public interface FileTransferRequestCallBack {
        void onTransferring(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode);
        void onReplicating(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode);
        void onTransmitted(List<FileBucket> buckets, PhysicalNode from, PhysicalNode toNode);
    }
}
