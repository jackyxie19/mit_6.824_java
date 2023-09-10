package worker;


public class MapResultManager {
    FileReadWrite readWrite;

    Worker worker;

    public MapResultManager(Worker worker){
        this.worker = worker;
        readWrite = new FileReadWriteLocal();
    }
    public void write(String mapTaskId, String result){
        String mapPath = assembleMapPath(mapTaskId);
        readWrite.write(mapPath, result);
    }

    public String read(String mapTaskId){
        String path = assembleMapPath(mapTaskId);
        String result = readWrite.read(path);
        return result;
    }

    private String assembleMapPath(String mapTaskId) {
        String classPath = System.getProperty("java.class.path");
        return classPath + "/" + worker.getWorkerId() + "/" + "mapResult" + "/" + mapTaskId;
    }
}
