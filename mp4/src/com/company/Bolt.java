package com.company;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.io.*;

/* Bolt
 * Functionality: This thread is created as bolt
 * that receives tuple from either other bolts or
 * static database(file), does something indicated
 * by client, and in some cases, send the computed
 * value to other bolts
 */
public class Bolt extends Thread {
    public String id;
    private int local_port;
    private String bolt_info;
    private ArrayList<String> parselist;
    private int network_flag = 0;
    private InetAddress output_addr;
    private int output_port;
    private File result;
    private InetAddress master;

    private Socket worker_sock;
    private OutputStream os;

    public Bolt(String info, InetAddress curr_master){
        master = curr_master;
        bolt_info = info;
    }

    public void compile() throws Exception{
        ArrayList<String> command = new ArrayList<>();
        command.add("g++");
        command.add("-std=c++0x");
        command.add(id + ".cpp");
        command.add("-o");
        command.add(id);
        ProcessBuilder builder = new ProcessBuilder(command);
        Process process = builder.start();
        process.waitFor();
    }

    public String get_own_ip(){
        String ret= "";
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            ret = socket.getLocalAddress().getHostAddress();
        }
        catch (Exception e0){ e0.printStackTrace();}
        return ret;
    }

    /* join_op()
     * Function: This function deals with the special
     * case of join operations, where another input
     * from a static database(file) is required
     * */
    public void join_op() throws Exception{
        System.out.println("join op");
        /* Wait for converge on the SDFS */
        Thread.sleep(2000);
        /* get db file and read */
        String dbname = "";
        if(bolt_info.split("\\s+")[2].contains(".db"))
            dbname = bolt_info.split("\\s+")[2];
        else if(bolt_info.split("\\s+")[3].contains(".db"))
            dbname = bolt_info.split("\\s+")[3];
        else if(bolt_info.split("\\s+").length == 5){
            if(bolt_info.split("\\s+")[4].contains(".db"))
                dbname = bolt_info.split("\\s+")[4];
        }

        compile();

        System.out.println("Reading file: " + dbname);
        /* establish connection */
        ServerSocket sock = new ServerSocket(local_port);
        Socket bolt_sock = sock.accept();
        InputStream is = bolt_sock.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        result  = new File(id + ".result");
        FileOutputStream fos = new FileOutputStream(result);

        String line = reader.readLine();


        if(network_flag == 2) {
            int flag = 0;
            while(flag == 0){
                try{
                    worker_sock = new Socket(output_addr, output_port);
                    flag = 1;
                }
                catch (IOException e){flag = 0;}
            }
            os = worker_sock.getOutputStream();
        }

        int EOS = 0;
        while(true){
            if(line == null){
                break;
            }
            if(line.equals("@END")){
                if(network_flag== 2){
                    os.write((line + "\n").getBytes());
                    os.flush();
                }
                EOS = 1;
                break;
            }
            System.out.println("got line: " + line);
            File db = new File(dbname);
            BufferedReader dbreader = new BufferedReader(new InputStreamReader(new FileInputStream(db)));
            String dbline = dbreader.readLine();
            while(dbline != null){
                /* execute client input program */
                ArrayList<String> command = new ArrayList<>();
                command.add("./" + id);
//                command.add(id + ".py");
                command.add(line.trim());
                command.add(dbline.trim());
                ProcessBuilder builder = new ProcessBuilder(command);
                Process process = builder.start();
                InputStreamReader isr = new InputStreamReader(process.getInputStream());
                BufferedReader br = new BufferedReader(isr);
                String outline;

                if(Thread.interrupted()){
                    System.out.println("Ending bolt");
                    sock.close();
                    if(network_flag == 2)
                        worker_sock.close();
                    return;
                }

                if(network_flag== 2){
                    while ((outline = br.readLine()) != null) {
                        outline += '\n';
//                        System.out.println("sending to "+output_addr.toString()+Integer.toString(output_port));
                        System.out.println(outline);
                        try{
                            os.write(outline.getBytes());
                            os.flush();
                        }
                        catch(IOException e){
                            System.out.println("Ending bolt");
                            sock.close();
                            if(network_flag == 2)
                                worker_sock.close();
                            return;
                        }
                    }

                }
                else {
                    while ((outline = br.readLine()) != null) {
//                        System.out.println("getting output sink");
                        System.out.println(outline);
                        outline += "\n";
                        fos.write(outline.getBytes());
                        fos.flush();
                    }
                }

                dbline = dbreader.readLine();
            }

            line = reader.readLine();
        }
        sock.close();
        if(network_flag == 2)
            worker_sock.close();

        if(EOS == 0){
            System.out.println("Ending bolt");
            return;
        }

        /* tell master that bolt is done */
        System.out.println("Ack to master: " + master);
        Socket reply_sock = new Socket(master, 5000);
        os = reply_sock.getOutputStream();
        DataOutputStream dos = new DataOutputStream(os);
        dos.writeUTF("Done");
        os.close();
        reply_sock.close();

        System.out.println("Join bolt finished");

        if(result.length() == 0){
            result.delete();
            return;
        }
        /* store result to SDFS */

        Socket sdfs_sock = new Socket(InetAddress.getByName(get_own_ip()), 6000);
        OutputStream os = sdfs_sock.getOutputStream();
        dos = new DataOutputStream(os);
        String cmd = "put " + id + ".result " + id + ".result";
        dos.writeUTF(cmd);
        os.close();
        sdfs_sock.close();
    }

    public void run(){
        parselist = new ArrayList<>();
        System.out.println(bolt_info);
        id = bolt_info.split("\\s+")[0];
        local_port = Integer.parseInt(bolt_info.split("\\s+")[1]);
        ServerSocket sock;

        int argnum = bolt_info.split("\\s+").length;
        switch(argnum){
            case 0:
            case 1:
            case 2:
                return;
            case 3:
                network_flag = 1;
                System.out.println("sink");
                break;
            case 4:
                String operand1 = bolt_info.split("\\s+")[2];
                String operand2 = bolt_info.split("\\s+")[3];
                /* In case of join sink */
                if(operand1.contains(".db") || operand2.contains(".db")){
                    network_flag = 1;
                    System.out.println("sink");
                    try{
                        join_op();
                    }
                    catch(Exception e){
                        e.printStackTrace();
                    }
                    return;
                }
                else{
                    network_flag = 2;
                    System.out.println("normal node");
                    String outputpack = bolt_info.split("\\s+")[2];
                    int mid = outputpack.indexOf(':');
                    try {
                        output_addr = InetAddress.getByName(outputpack.substring(1, mid));
                    }
                    catch(Exception e){e.printStackTrace();}
                    output_port = Integer.parseInt(outputpack.substring(mid+1));
                    break;
                }
            case 5:
                network_flag = 2;
                System.out.println("normal node");
                String outputpack = bolt_info.split("\\s+")[2];
                int mid = outputpack.indexOf(':');
                try {
                    output_addr = InetAddress.getByName(outputpack.substring(1, mid));
                }
                catch(Exception e){e.printStackTrace();}
                output_port = Integer.parseInt(outputpack.substring(mid+1));
                try{
                    join_op();
                }
                catch(Exception e){
                    e.printStackTrace();
                }
                return;
        }

        try{
            System.out.println("binding" + local_port);
            sock = new ServerSocket(local_port);
            Socket bolt_sock = sock.accept();
            InputStream is = bolt_sock.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));

            compile();

            result  = new File(id + ".result");
            FileOutputStream fos = new FileOutputStream(result);


            String line = reader.readLine();

            Thread.sleep(500);

            if(network_flag == 2) {
                int flag = 0;
                while(flag == 0){
                    try{
                        worker_sock = new Socket(output_addr, output_port);
                        flag = 1;
                    }
                    catch (IOException e){flag = 0;}
                }
                os = worker_sock.getOutputStream();
            }

            int EOS = 0;
            while(true){
//                System.out.println("Line: " + 294);
                if(line == null)
                    break;
                if(line.equals("@END")){
                    if(network_flag== 2){
                        os.write((line + "\n").getBytes());
                        os.flush();
                    }
                    EOS = 1;
                    break;
                }
                /* execute client input program */
                ArrayList<String> command = new ArrayList<>();
                command.add("./" + id);
//                command.add(id + ".py");
                command.add(line.trim());
                ProcessBuilder builder = new ProcessBuilder(command);
                Process process = builder.start();
                InputStreamReader isr = new InputStreamReader(process.getInputStream());
                BufferedReader br = new BufferedReader(isr);
                String outline;
//                System.out.println("Line: " + 317);
                if(Thread.interrupted()){
                    System.out.println("Ending bolt");
                    sock.close();
                    if(network_flag == 2)
                        worker_sock.close();
                    return;
                }

                if(network_flag== 2){
                    while ((outline = br.readLine()) != null) {
                        outline += '\n';
//                        System.out.println("sending to "+output_addr.toString()+Integer.toString(output_port));
                        System.out.println(outline);
//                        System.out.println("Line: " + 329);
                        try{
                            os.write(outline.getBytes());
                            os.flush();
                        }
                        catch(IOException e){
                            System.out.println("Ending bolt");
                            sock.close();
                            return;
                        }
                    }
                }
                else {
                    while ((outline = br.readLine()) != null) {
//                        System.out.println("getting output sink");
                        System.out.println(outline);
                        outline += "\n";
                        fos.write(outline.getBytes());
                        fos.flush();
                    }
                }
//                System.out.println("Line: " + 350);
                line = reader.readLine();
            }
            sock.close();
            if(network_flag == 2)
                worker_sock.close();

            if(EOS == 0){
                System.out.println("Ending bolt");
                return;
            }


            if(Thread.interrupted()){
                System.out.println("Ending bolt");
                return;
            }

            /* tell master that bolt is done */
            System.out.println("Ack to master: " + master);
            Socket reply_sock = new Socket(master, 5000);
            os = reply_sock.getOutputStream();
            DataOutputStream dos = new DataOutputStream(os);
            dos.writeUTF("Done");
            os.close();
            reply_sock.close();
            System.out.println("Bolt finished");

            if(result.length() == 0){
                result.delete();
                return;
            }
            /* store result to SDFS */
            Socket sdfs_sock = new Socket(InetAddress.getByName(get_own_ip()), 6000);
            OutputStream os = sdfs_sock.getOutputStream();
            dos = new DataOutputStream(os);
            String cmd = "put " + id + ".result " + id + ".result";
            dos.writeUTF(cmd);
            os.close();
            sdfs_sock.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
