package client;

import master.Master;

public class Client {
    public static void main(String[] args) {
        String file = "asdasdasd asdfasdfa";
        ClientRequest clientRequest = new ClientRequest();

        Master master = new Master();
        master.submitJob(clientRequest);
    }
}
