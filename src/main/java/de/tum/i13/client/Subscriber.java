package de.tum.i13.client;

import de.tum.i13.shared.B64Util;

public class Subscriber {

    public void handleRequests(String command) {

            String[] request = command.split(" ");
            if (request != null) {
                switch (request[0]) {
                    case ("subscribe_delete"):
                        System.out.println("Subscribed key deleted: "+ B64Util.b64decode(request[1]));
                        break;
                    case ("subscribe_update"):
                        System.out.println("Subscribed key updated: key: "+ B64Util.b64decode(request[1]) +" value: " +  B64Util.b64decode(request[2]));
                        break;
                    default:
                        break;
                }
            }
    }

}
