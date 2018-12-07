package com.company;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

public class Ping_receiver extends Thread {
    private DatagramSocket pingSocket;

    public Ping_receiver() throws IOException {
        pingSocket = new DatagramSocket(3600);
    }

    public void run() {
        while (true) {
            try{
                byte[] msg = new byte[200];
                DatagramPacket packet = new DatagramPacket(msg, msg.length);
                pingSocket.receive(packet);
                InetAddress clientAddress = packet.getAddress();         //get the address of the sender
                int port = packet.getPort();
                String recved = new String(packet.getData(), StandardCharsets.UTF_8);
                if (recved.substring(0, 4).equals("ping")) {                                //if ping, reply with pingack
                    String str = "pingack";
                    byte[] sending_msg = str.getBytes();
                    DatagramPacket sending = new DatagramPacket(sending_msg, sending_msg.length, clientAddress, port);
                    pingSocket.send(sending);
                }
            }
            catch(IOException e){}
        }
    }
}
