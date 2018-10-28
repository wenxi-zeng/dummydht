package filemanagement;

import commonmodels.PhysicalNode;
import util.SimpleLog;

import java.util.List;

import static util.Config.NUMBER_OF_HASH_SLOTS;

public class FileTransferManager {

    private LocalFileManager localFileManager;

    private static volatile FileTransferManager instance = null;

    private FileTransferManager() {
        localFileManager = LocalFileManager.getInstance();
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

    public void transfer(int hi, int hf, PhysicalNode from, PhysicalNode toNode) {
        int numberOfFilesTransferred = 0;
        long sizeOfFilesTransferred = 0;
        if (hf < hi) {
            for (int bucket = hi + 1; bucket <= NUMBER_OF_HASH_SLOTS; bucket++) {
                FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
                numberOfFilesTransferred += fileBucket.getNumberOfFiles();
                sizeOfFilesTransferred += fileBucket.getSize();
            }
            for (int bucket = 0; bucket <= hf; bucket++) {
                FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
                numberOfFilesTransferred += fileBucket.getNumberOfFiles();
                sizeOfFilesTransferred += fileBucket.getSize();
            }
        }
        else {
            for (int bucket = hi + 1; bucket <= hf; bucket++) {
                FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
                numberOfFilesTransferred += fileBucket.getNumberOfFiles();
                sizeOfFilesTransferred += fileBucket.getSize();
            }
        }

        SimpleLog.i("Message from : " + from.getId() + ": Transfer to " + toNode.getId() + " completed. Number of files transferred: " + numberOfFilesTransferred + ", Total size: " + sizeOfFilesTransferred);
    }

    public void transfer(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        int numberOfFilesTransferred = 0;
        long sizeOfFilesTransferred = 0;

        for (int bucket : buckets ){
            FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
            numberOfFilesTransferred += fileBucket.getNumberOfFiles();
            sizeOfFilesTransferred += fileBucket.getSize();
        }

        SimpleLog.i("Message from : " + from.getId() + ": Transfer to " + toNode.getId() + " completed. Number of files transferred: " + numberOfFilesTransferred + ", Total size: " + sizeOfFilesTransferred);
    }

    public void transfer(int bucket, PhysicalNode from, PhysicalNode toNode) {
        int numberOfFilesTransferred = 0;
        long sizeOfFilesTransferred = 0;

        FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
        numberOfFilesTransferred += fileBucket.getNumberOfFiles();
        sizeOfFilesTransferred += fileBucket.getSize();

        SimpleLog.i("Message from : " + from.getId() + ": Transfer to " + toNode.getId() + " completed. Number of files transferred: " + numberOfFilesTransferred + ", Total size: " + sizeOfFilesTransferred);
    }

    public void copy(int hi, int hf, PhysicalNode from, PhysicalNode toNode) {
        int numberOfFilesReplicated = 0;
        long sizeOfFilesRelicated = 0;
        if (hf < hi) {
            for (int bucket = hi + 1; bucket <= NUMBER_OF_HASH_SLOTS; bucket++) {
                FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
                numberOfFilesReplicated += fileBucket.getNumberOfFiles();
                sizeOfFilesRelicated += fileBucket.getSize();
            }
            for (int bucket = 0; bucket <= hf; bucket++) {
                FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
                numberOfFilesReplicated += fileBucket.getNumberOfFiles();
                sizeOfFilesRelicated += fileBucket.getSize();
            }
        }
        else {
            for (int bucket = hi + 1; bucket <= hf; bucket++) {
                FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
                numberOfFilesReplicated += fileBucket.getNumberOfFiles();
                sizeOfFilesRelicated += fileBucket.getSize();
            }
        }

        SimpleLog.i("Message from : " + from.getId() + ": Replicate files to " + toNode.getId() + " completed. Number of files replicated: " + numberOfFilesReplicated + ", Total size: " + sizeOfFilesRelicated);
    }

    public void copy(List<Integer> buckets, PhysicalNode from, PhysicalNode toNode) {
        int numberOfFilesReplicted = 0;
        long sizeOfFilesReplicated = 0;

        for (int bucket : buckets ){
            FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
            numberOfFilesReplicted += fileBucket.getNumberOfFiles();
            sizeOfFilesReplicated += fileBucket.getSize();
        }

        SimpleLog.i("Message from : " + from.getId() + ": Replicate files to " + toNode.getId() + "Transfer completed. Number of files transferred: " + numberOfFilesReplicted + ", Total size: " + sizeOfFilesReplicated);
    }

    public void copy(int bucket, PhysicalNode from, PhysicalNode toNode) {
        int numberOfFilesReplicted = 0;
        long sizeOfFilesReplicated = 0;

        FileBucket fileBucket = localFileManager.getLocalBuckets().get(bucket);
        numberOfFilesReplicted += fileBucket.getNumberOfFiles();
        sizeOfFilesReplicated += fileBucket.getSize();

        SimpleLog.i("Message from : " + from.getId() + ": Replicate files to " + toNode.getId() + "Transfer completed. Number of files transferred: " + numberOfFilesReplicted + ", Total size: " + sizeOfFilesReplicated);
    }
}
