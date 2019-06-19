/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.fusesource.hawtbuf.*;
import org.fusesource.mqtt.client.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;

/**
 * @author xieyu
 * Uses an callback based interface to MQTT.  Callback based interfaces
 * are harder to use but are slightly more efficient.
 * Receive frames and combine them to a file. Use shell to load the image from that file.
 */
class Listener {

    public static void main(String []args) throws Exception {

        String user = env("ACTIVEMQ_USER", "admin");
        String password = env("ACTIVEMQ_PASSWORD", "password");
        String host = env("ACTIVEMQ_HOST", "10.109.17.251");
        int port = Integer.parseInt(env("ACTIVEMQ_PORT", "1883"));
        final String destination = arg(args, 0, "docker");


        MQTT mqtt = new MQTT();
        mqtt.setHost(host, port);
        mqtt.setUserName(user);
        mqtt.setPassword(password);

        final CallbackConnection connection = mqtt.callbackConnection();
        connection.listener(new org.fusesource.mqtt.client.Listener() {
            long count = 0;
            long start = System.currentTimeMillis();

            public void onConnected() {
            }
            public void onDisconnected() {
            }
            public void onFailure(Throwable value) {
                value.printStackTrace();
                System.exit(-2);
            }
            public void onPublish(UTF8Buffer topic, Buffer msg, Runnable ack) {

                byte[] bt = msg.toByteArray();
                BufferedOutputStream bos=null;
                FileOutputStream fos=null;
                File file=null;

                if(Arrays.equals(bt,"SHUTDOWN".getBytes())) {
                    long diff = System.currentTimeMillis() - start;
                    ack.run();
                    System.out.println(String.format("Received %d in %.2f seconds", count, (1.0*diff/1000.0)));
                    connection.disconnect(new Callback<Void>() {
                        @Override
                        public void onSuccess(Void value) {
                            try {
                                // Set shell path
                                String path="/Users/xieyu/Downloads/load.sh";
                                File command = new File(path);
                                FileOutputStream fos = new FileOutputStream(command);
                                BufferedOutputStream bs=new BufferedOutputStream(fos);

                                // Write content of shell.
                                // The filepath need to be consistent with file saved
                                String top = "#!/bin/bash\n";
                                String s = top + "docker load -i /Users/xieyu/Downloads/registry1.tar";
                                bs.write(s.getBytes());
                                bs.close();
                                fos.close();

                                // Execute shell file
                                Process ps = Runtime.getRuntime().exec("chmod 777 " + path);
                                ps.waitFor();
                                ps = Runtime.getRuntime().exec(path);
                                ps.waitFor();

                            }catch (Exception e){
                                e.fillInStackTrace();
                            }
                            System.exit(0);
                        }
                        @Override
                        public void onFailure(Throwable value) {
                            value.printStackTrace();
                            System.exit(-2);
                        }
                    });
                } else {
                    if( count == 0 ) {
                        start = System.currentTimeMillis();
                    }
                    count ++;
                    System.out.println(String.format("Received %d messages.", count));
                    try {
                        file=new File("/Users/xieyu/Downloads/registry1.tar");
                        fos=new FileOutputStream(file,true);
                        bos=new BufferedOutputStream(fos);
                        bos.write(bt);
                        bos.close();
                        fos.close();
                    }catch (Exception e){
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                    }
                }
                ack.run();
            }
        });
        connection.connect(new Callback<Void>() {
            @Override
            public void onSuccess(Void value) {
                Topic[] topics = {new Topic(destination, QoS.EXACTLY_ONCE)};
                connection.subscribe(topics, new Callback<byte[]>() {
                    public void onSuccess(byte[] qoses) {
                    }
                    public void onFailure(Throwable value) {
                        value.printStackTrace();
                        System.exit(-2);
                    }
                });
            }
            @Override
            public void onFailure(Throwable value) {
                value.printStackTrace();
                System.exit(-2);
            }
        });

        // Wait forever..
        synchronized (Listener.class) {
            while(true)
                Listener.class.wait();
        }
    }

    private static String env(String key, String defaultValue) {
        String rc = System.getenv(key);
        if( rc== null )
            return defaultValue;
        return rc;
    }

    private static String arg(String []args, int index, String defaultValue) {
        if( index < args.length )
            return args[index];
        else
            return defaultValue;
    }
}