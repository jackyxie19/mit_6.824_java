package worker;

public class ReduceResultManager {
    FileReadWrite readWrite;
    Worker worker;

    public ReduceResultManager(Worker worker) {
        this.worker = worker;
        readWrite = new FileReadWriteLocal();
    }

    public void write(String reduceTaskId, String result){

    }

    public String read(String reduceTaskId){
        String path = assembleReducePath(reduceTaskId);
        String result = readWrite.read(path);
        return result;
    }

    private String assembleReducePath(String reduceTaskId) {
        return null;
    }
}
