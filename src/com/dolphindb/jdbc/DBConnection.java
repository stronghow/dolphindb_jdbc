package com.dolphindb.jdbc;

import com.xxdb.data.BasicEntityFactory;
import com.xxdb.data.Entity;
import com.xxdb.data.EntityFactory;
import com.xxdb.data.Void;
import com.xxdb.io.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class DBConnection {
    private static final int MAX_FORM_VALUE = Entity.DATA_FORM.values().length - 1;
    private static final int MAX_TYPE_VALUE = Entity.DATA_TYPE.values().length - 1;
    private ReentrantLock mutex = new ReentrantLock();
    private String sessionID = "";
    private Socket socket;
    private boolean remoteLittleEndian;
    private ExtendedDataOutput out;
    private EntityFactory factory = new BasicEntityFactory();
    private String hostName;
    private int port;

    public DBConnection() {
    }

    public boolean isBusy() {
        if (!this.mutex.tryLock()) {
            return true;
        } else {
            this.mutex.unlock();
            return false;
        }
    }

    public boolean connect(String hostName, int port) throws IOException {
        this.mutex.lock();

        try {
            if (this.sessionID.isEmpty()) {
                this.hostName = hostName;
                this.port = port;
                this.socket = new Socket(hostName, port);
                this.socket.setTcpNoDelay(true);
                this.out = new LittleEndianDataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
                ExtendedDataInput in = new LittleEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream()));
                String body = "connect\n";
                this.out.writeBytes("API 0 ");
                this.out.writeBytes(String.valueOf(body.length()));
                this.out.writeByte(10);
                this.out.writeBytes(body);
                this.out.flush();
                String line = in.readLine();
                int endPos = line.indexOf(32);
                if (endPos <= 0) {
                    this.close();
                    return false;
                }

                this.sessionID = line.substring(0, endPos);
                int startPos = endPos + 1;
                endPos = line.indexOf(32, startPos);
                if (endPos != line.length() - 2) {
                    this.close();
                    return false;
                }

                if (line.charAt(endPos + 1) == '0') {
                    this.remoteLittleEndian = false;
                    this.out = new BigEndianDataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
                } else {
                    this.remoteLittleEndian = true;
                }

                return true;
            }

            this.mutex.unlock();
        } finally {
            this.mutex.unlock();
        }

        return true;
    }

    public boolean getRemoteLittleEndian() {
        return this.remoteLittleEndian;
    }

    public Entity tryRun(String script) throws IOException {
        if (!this.mutex.tryLock()) {
            return null;
        } else {
            Entity var3;
            try {
                var3 = this.run(script);
            } finally {
                this.mutex.unlock();
            }

            return var3;
        }
    }

    public Entity run(String script) throws IOException {
        return this.run(script, (ProgressListener)null);
    }

    public boolean tryReconnect() throws IOException {
        this.socket = new Socket(this.hostName, this.port);
        this.out = new LittleEndianDataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
        ExtendedDataInput in = new LittleEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream()));
        String body = "connect\n";
        this.out.writeBytes("API 0 ");
        this.out.writeBytes(String.valueOf(body.length()));
        this.out.writeByte(10);
        this.out.writeBytes(body);
        this.out.flush();
        String line = in.readLine();
        int endPos = line.indexOf(32);
        if (endPos <= 0) {
            this.close();
            return false;
        } else {
            this.sessionID = line.substring(0, endPos);
            int startPos = endPos + 1;
            endPos = line.indexOf(32, startPos);
            if (endPos != line.length() - 2) {
                this.close();
                return false;
            } else {
                if (line.charAt(endPos + 1) == '0') {
                    this.remoteLittleEndian = false;
                    this.out = new BigEndianDataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
                } else {
                    this.remoteLittleEndian = true;
                }

                return true;
            }
        }
    }

    public Entity run(String script, ProgressListener listener) throws IOException {
        this.mutex.lock();

        Void var16;
        try {
            boolean reconnect = false;
            if (this.socket == null || !this.socket.isConnected() || this.socket.isClosed()) {
                if (this.sessionID.isEmpty()) {
                    throw new IOException("Database connection is not established yet.");
                }

                this.socket = new Socket(this.hostName, this.port);
                this.out = new LittleEndianDataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
            }

            System.out.println("socket create");
            String body = "script\n" + script;
            System.out.println("run script : " + script);
            ExtendedDataInput in = null;
            String header = null;

            try {
                this.out.writeBytes((listener != null ? "API2 " : "API ") + this.sessionID + " ");
                this.out.writeBytes(String.valueOf(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0)));
                this.out.writeByte(10);
                this.out.writeBytes(body);
                this.out.flush();
                System.out.println("send connected command");
                in = this.remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream())) : new BigEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream()));
                header = ((ExtendedDataInput)in).readLine();
                System.out.println("read header");
            } catch (IOException var22) {
                if (reconnect) {
                    this.socket = null;
                    throw var22;
                }

                try {
                    this.tryReconnect();
                    this.out.writeBytes((listener != null ? "API2 " : "API ") + this.sessionID + " ");
                    this.out.writeBytes(String.valueOf(AbstractExtendedDataOutputStream.getUTFlength(body, 0, 0)));
                    this.out.writeByte(10);
                    this.out.writeBytes(body);
                    this.out.flush();
                    in = this.remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream())) : new BigEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream()));
                    header = ((ExtendedDataInput)in).readLine();
                    reconnect = true;
                } catch (Exception var21) {
                    this.socket = null;
                    throw var21;
                }
            }

            for(; header.equals("MSG"); header = ((ExtendedDataInput)in).readLine()) {
                String msg = ((ExtendedDataInput)in).readString();
                if (listener != null) {
                    listener.progress(msg);
                }
            }

            String[] headers = header.split(" ");
            if (headers.length != 3) {
                this.socket = null;
                throw new IOException("Received invalid header: " + header);
            }

            if (reconnect) {
                this.sessionID = headers[0];
            }

            int numObject = Integer.parseInt(headers[1]);
            String msg = ((ExtendedDataInput)in).readLine();
            System.out.println("read msg" + msg);
            if (!msg.equals("OK")) {
                throw new IOException(msg);
            }

            if (numObject != 0) {
                try {
                    short flag = ((ExtendedDataInput)in).readShort();
                    System.out.println("read readShort" + msg);
                    int form = flag >> 8;
                    int type = flag & 255;
                    if (form >= 0 && form <= MAX_FORM_VALUE) {
                        if (type >= 0 && type <= MAX_TYPE_VALUE) {
                            Entity.DATA_FORM df = Entity.DATA_FORM.values()[form];
                            Entity.DATA_TYPE dt = Entity.DATA_TYPE.values()[type];
                            Entity var26 = this.factory.createEntity(df, dt, (ExtendedDataInput)in);
                            return var26;
                        }

                        throw new IOException("Invalid type value: " + type);
                    }

                    throw new IOException("Invalid form value: " + form);
                } catch (IOException var23) {
                    this.socket = null;
                    throw var23;
                }
            }

            var16 = new Void();
        } finally {
            this.mutex.unlock();
        }

        return var16;
    }

    public Entity tryRun(String function, List<Entity> arguments) throws IOException {
        if (!this.mutex.tryLock()) {
            return null;
        } else {
            Entity var4;
            try {
                var4 = this.run(function, arguments);
            } finally {
                this.mutex.unlock();
            }

            return var4;
        }
    }

    public Entity run(String function, List<Entity> arguments) throws IOException {
        this.mutex.lock();

        Void var15;
        try {
            boolean reconnect = false;
            if (this.socket == null || !this.socket.isConnected() || this.socket.isClosed()) {
                if (this.sessionID.isEmpty()) {
                    throw new IOException("Database connection is not established yet.");
                }

                this.socket = new Socket(this.hostName, this.port);
                this.out = new LittleEndianDataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
            }

            String body = "function\n" + function;
            body = body + "\n" + arguments.size() + "\n";
            body = body + (this.remoteLittleEndian ? "1" : "0");
            ExtendedDataInput in = null;
            String[] headers = null;

            int numObject;
            try {
                this.out.writeBytes("API " + this.sessionID + " ");
                this.out.writeBytes(String.valueOf(body.length()));
                this.out.writeByte(10);
                this.out.writeBytes(body);

                for(numObject = 0; numObject < arguments.size(); ++numObject) {
                    ((Entity)arguments.get(numObject)).write(this.out);
                }

                this.out.flush();
                in = this.remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream())) : new BigEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream()));
                headers = ((ExtendedDataInput)in).readLine().split(" ");
            } catch (IOException var21) {
                if (reconnect) {
                    this.socket = null;
                    throw var21;
                }

                try {
                    this.tryReconnect();
                    this.out = new LittleEndianDataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
                    this.out.writeBytes("API " + this.sessionID + " ");
                    this.out.writeBytes(String.valueOf(body.length()));
                    this.out.writeByte(10);
                    this.out.writeBytes(body);

                    for(int i = 0; i < arguments.size(); ++i) {
                        ((Entity)arguments.get(i)).write(this.out);
                    }

                    this.out.flush();
                    in = this.remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream())) : new BigEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream()));
                    headers = ((ExtendedDataInput)in).readLine().split(" ");
                    reconnect = true;
                } catch (Exception var20) {
                    this.socket = null;
                    throw var20;
                }
            }

            if (headers.length != 3) {
                this.socket = null;
                throw new IOException("Received invalid header.");
            }

            if (reconnect) {
                this.sessionID = headers[0];
            }

            numObject = Integer.parseInt(headers[1]);
            String msg = ((ExtendedDataInput)in).readLine();
            if (!msg.equals("OK")) {
                throw new IOException(msg);
            }

            if (numObject != 0) {
                try {
                    short flag = ((ExtendedDataInput)in).readShort();
                    int form = flag >> 8;
                    int type = flag & 255;
                    if (form >= 0 && form <= MAX_FORM_VALUE) {
                        if (type >= 0 && type <= MAX_TYPE_VALUE) {
                            Entity.DATA_FORM df = Entity.DATA_FORM.values()[form];
                            Entity.DATA_TYPE dt = Entity.DATA_TYPE.values()[type];
                            Entity var25 = this.factory.createEntity(df, dt, (ExtendedDataInput)in);
                            return var25;
                        }

                        throw new IOException("Invalid type value: " + type);
                    }

                    throw new IOException("Invalid form value: " + form);
                } catch (IOException var22) {
                    this.socket = null;
                    throw var22;
                }
            }

            var15 = new Void();
        } finally {
            this.mutex.unlock();
        }

        return var15;
    }

    public void tryUpload(Map<String, Entity> variableObjectMap) throws IOException {
        if (!this.mutex.tryLock()) {
            throw new IOException("The connection is in use.");
        } else {
            try {
                this.upload(variableObjectMap);
            } finally {
                this.mutex.unlock();
            }

        }
    }

    public void upload(Map<String, Entity> variableObjectMap) throws IOException {
        if (variableObjectMap != null && !variableObjectMap.isEmpty()) {
            this.mutex.lock();

            try {
                boolean reconnect = false;
                if (this.socket == null || !this.socket.isConnected() || this.socket.isClosed()) {
                    if (this.sessionID.isEmpty()) {
                        throw new IOException("Database connection is not established yet.");
                    }

                    reconnect = true;
                    this.socket = new Socket(this.hostName, this.port);
                    this.out = new LittleEndianDataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
                }

                List<Entity> objects = new ArrayList();
                String body = "variable\n";
                Iterator var6 = variableObjectMap.keySet().iterator();

                while(var6.hasNext()) {
                    String key = (String)var6.next();
                    if (!this.isVariableCandidate(key)) {
                        throw new IllegalArgumentException("'" + key + "' is not a good variable name.");
                    }

                    body = body + key + ",";
                    objects.add((Entity)variableObjectMap.get(key));
                }

                body = body.substring(0, body.length() - 1);
                body = body + "\n" + objects.size() + "\n";
                body = body + (this.remoteLittleEndian ? "1" : "0");

                try {
                    this.out.writeBytes("API " + this.sessionID + " ");
                    this.out.writeBytes(String.valueOf(body.length()));
                    this.out.writeByte(10);
                    this.out.writeBytes(body);

                    for(int i = 0; i < objects.size(); ++i) {
                        ((Entity)objects.get(i)).write(this.out);
                    }

                    this.out.flush();
                } catch (IOException var13) {
                    if (reconnect) {
                        this.socket = null;
                        throw var13;
                    }

                    try {
                        this.socket = new Socket(this.hostName, this.port);
                        this.out = new LittleEndianDataOutputStream(new BufferedOutputStream(this.socket.getOutputStream()));
                        this.out.writeBytes("API " + this.sessionID + " ");
                        this.out.writeBytes(String.valueOf(body.length()));
                        this.out.writeByte(10);
                        this.out.writeBytes(body);

                        for(int i = 0; i < objects.size(); ++i) {
                            ((Entity)objects.get(i)).write(this.out);
                        }

                        this.out.flush();
                        reconnect = true;
                    } catch (Exception var12) {
                        this.socket = null;
                        throw var12;
                    }
                }

                ExtendedDataInput in = this.remoteLittleEndian ? new LittleEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream())) : new BigEndianDataInputStream(new BufferedInputStream(this.socket.getInputStream()));
                String[] headers = ((ExtendedDataInput)in).readLine().split(" ");
                if (headers.length != 3) {
                    this.socket = null;
                    throw new IOException("Received invalid header.");
                }

                if (reconnect) {
                    this.sessionID = headers[0];
                }

                String msg = ((ExtendedDataInput)in).readLine();
                if (!msg.equals("OK")) {
                    throw new IOException(msg);
                }
            } finally {
                this.mutex.unlock();
            }

        }
    }

    public boolean isClosed(){
        return this.socket == null || !this.socket.isConnected() || this.socket.isClosed();
    }

    public void close() {
        this.mutex.lock();

        try {
            if (this.socket != null) {
                this.socket.close();
                this.sessionID = "";
                this.socket = null;
            }
        } catch (Exception var5) {
            var5.printStackTrace();
        } finally {
            this.mutex.unlock();
        }

    }

    private boolean isVariableCandidate(String word) {
        char cur = word.charAt(0);
        if ((cur < 'a' || cur > 'z') && (cur < 'A' || cur > 'Z')) {
            return false;
        } else {
            for(int i = 1; i < word.length(); ++i) {
                cur = word.charAt(i);
                if ((cur < 'a' || cur > 'z') && (cur < 'A' || cur > 'Z') && (cur < '0' || cur > '9') && cur != '_') {
                    return false;
                }
            }

            return true;
        }
    }

    public String getHostName() {
        return this.hostName;
    }

    public int getPort() {
        return this.port;
    }

    public InetAddress getLocalAddress() {
        return this.socket.getLocalAddress();
    }
}
