use redis::Commands;
use serde_json::{json, Result, Value};
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::TcpStream;
use std::str;
use std::sync::mpsc;
use std::thread;
use uuid::Uuid;

use crate::message::{Message, ServiceMessage, ServiceMsgType, ServiceType};

pub fn read_storage(stream: &mut TcpStream, msg: Value) {
    // TODO Fetch details of the file from the core_server
    let client = redis::Client::open("redis://172.28.5.3/7").unwrap();
    let mut con = client.get_connection().unwrap();

    // TODO calculate the hash of the file using the filename and the user UUID
    let user_file_uuid = msg["id"].as_str().unwrap().to_string();
    let jsondata: String = con.get(&user_file_uuid).unwrap();
    let jsonmap: HashMap<String, HashMap<String, Vec<String>>> =
        serde_json::from_str(&jsondata.as_str()).unwrap();

    let (sct_tx, sct_rx) = mpsc::channel();
    /*
    let rxthread = thread::spawn(move || {
        String::from("finished")
    });

    */

    let mut t_handles: Vec<thread::JoinHandle<_>> = vec![];
    for (k, v) in &jsonmap["data"] {
        let nextserver_ip = v[0].clone();
        let metadata: Value = serde_json::from_str(v[1].as_str()).unwrap();

        let size = metadata["size"].as_u64().unwrap() as usize;
        let off = metadata["offset"].as_u64().unwrap() as usize;
        let index = metadata["index"].as_u64().unwrap() as usize;

        let content = json!({
        "msg_type" :  "read",
        "metadata" :   metadata,
         })
        .to_string();
        println!("{}", content);

        let data = ServiceMessage {
            msg_type: ServiceMsgType::SERVICEINIT,
            service_type: ServiceType::Storage,
            content: content,
        };

        let msg_data = serde_json::to_string(&data).unwrap();

        let dup_sct_tx = mpsc::Sender::clone(&sct_tx);
        let storage_client_thread = thread::spawn(move || {
            let mut cstream = TcpStream::connect(nextserver_ip.clone()).unwrap();
            cstream.write_all(msg_data.as_bytes()).unwrap();
            cstream.flush().unwrap();

            let mut destbuffer = [0 as u8; 2048];
            let mut total = 0 as usize;
            let mut bufvec: Vec<u8> = vec![];
            loop {
                let dno = cstream.read(&mut destbuffer).unwrap();
                total += dno;
                println!("Total {} - dno {} - size {}", total, dno, size);
                bufvec.append(&mut destbuffer[0..dno].to_vec());
                if total == size {
                    break;
                }
            }

            dup_sct_tx
                .send(((size, index), bufvec, "data".to_string()))
                .unwrap();
        });

        t_handles.push(storage_client_thread);
    }

    for handle in t_handles {
        handle.join().unwrap();
    }
    sct_tx
        .send(((0 as usize, 0 as usize), vec![], "End".to_string()))
        .unwrap();

    // TODO store the total_size in the redis db  (this is a temp hack for a specific file)
    stream
        .write_all(
            json!({
                "total_size": 65548,
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap();
    stream.flush().unwrap();

    for received in sct_rx.iter() {
        match received {
            ((size, index), chunk, dat) => {
                let end: &String = &dat;
                let mut resp = [0; 512];
                if end.trim() == String::from("End") {
                    println!("{:?}", end.trim());
                    stream
                        .write_all(
                            json!({
                                "msg_type": "End",
                            })
                            .to_string()
                            .as_bytes(),
                        )
                        .unwrap();
                    break;
                }
                println!("Senting to client chunk of Size :- {} ", size);
                stream
                    .write_all(
                        json!({
                            "msg_type": "meta",
                            "size" : size,
                            "index" : index,
                        })
                        .to_string()
                        .as_bytes(),
                    )
                    .unwrap();
                stream.flush().unwrap();
                let no = stream.read(&mut resp).unwrap();

                if std::str::from_utf8(&resp[0..no]).unwrap() == "OK" {
                    stream.write_all(&chunk.as_slice()).unwrap();
                    stream.flush().unwrap();
                }
                // TODO
                // Combine the encrypted file chunks in correct order and decrypt it using some key
            }
            _ => (),
        };
    }

    //let ndata: String = rxthread.join().unwrap();

    // TODO Properly combine data
    // Send the file back to the user in json, also accessible from the website
    //respond_back(stream, String::from("OK").as_bytes());
}

pub fn write_storage(
    stream: &mut TcpStream,
    msg: Value,
    resp: Value,
) -> std::result::Result<String, ()> {
    println!("Writing file");
    let filesize = msg["filesize"].as_u64().unwrap();

    let mut destbuffer = [0 as u8; 512];
    // TODO Encrypt the file and split it to chunks of equal size
    let alloc_nodes = resp["response"]["node_ip"].as_array().unwrap();

    let mut node_array: Vec<String> = Vec::new();
    let mut nodedata: Vec<Vec<String>> = Vec::new();

    // Store the node ips to a
    for i in alloc_nodes.iter() {
        println!("{}", i);
        node_array.push(i.as_str().unwrap().split(":").collect::<Vec<&str>>()[0].to_string());
    }

    let mut destbuffer = [0 as u8; 2048];

    stream.write_all(String::from("OK").as_bytes()).unwrap();
    stream.flush().unwrap();

    let (sct_tx, sct_rx) = mpsc::channel();
    let (meta_tx, meta_rx) = mpsc::channel();

    let meta_thread = thread::spawn(move || {
        let mut chunklist: HashMap<String, Vec<String>> = HashMap::new();
        for received in meta_rx.iter() {
            match received {
                (metadata, ip) => {
                    //println!("Received meadata {}", metadata);
                    let end: &String = &metadata;
                    if end.trim() == String::from("End") {
                        println!("{:?}", end.trim());
                        break;
                    }

                    /*
                    let dno = forward_to(
                        coreserver_ip,
                        chunk_metadata,
                        &mut destbuffer,
                        &data,
                    );*/

                    // TODO calculate real hash of the chunk

                    let hash = Uuid::new_v4().to_string();
                    chunklist.insert(hash, vec![ip, metadata]);
                }
                _ => (),
            };
        }
        json!({
            "data": chunklist,
        })
        .to_string()
    });

    let dup_meta_tx = mpsc::Sender::clone(&meta_tx);
    let node_thread = thread::spawn(move || {
        let mut t_handles: Vec<thread::JoinHandle<_>> = vec![];
        for recv in sct_rx.iter() {
            match recv {
                (index, ip, chunkbuf) => {
                    let end: &String = &ip;
                    println!("{:?}", end.trim());
                    if end.trim() == String::from("End") {
                        for handle in t_handles {
                            handle.join().unwrap();
                        }
                        dup_meta_tx
                            .send((String::from("End"), String::from("")))
                            .unwrap();
                        break;
                    }
                    let nextserver_ip = format!("{}:7777", &ip);

                    let datachunk: Vec<u8> = chunkbuf;
                    println!(
                        "Index [{}] : Forwarding chunk {:?} ",
                        index,
                        datachunk.len()
                    );

                    let dup2_meta_tx = mpsc::Sender::clone(&dup_meta_tx);

                    let storage_client_thread = thread::spawn(move || {
                        let mut destbuffer = [0 as u8; 512];

                        let content = json!({
                        "msg_type" :  "write",
                        "size"     :   datachunk.len(),
                         })
                        .to_string();

                        let data = ServiceMessage {
                            msg_type: ServiceMsgType::SERVICEINIT,
                            service_type: ServiceType::Storage,
                            content: content,
                        };
                        let msg_data = serde_json::to_string(&data).unwrap();

                        let mut resp = [0; 512];

                        let mut cstream = TcpStream::connect(nextserver_ip.clone()).unwrap();
                        cstream.write_all(msg_data.as_bytes()).unwrap();
                        cstream.flush().unwrap();

                        let no = cstream.read(&mut resp).unwrap();

                        if std::str::from_utf8(&resp[0..no]).unwrap() == "OK" {
                            cstream.write_all(datachunk.as_slice()).unwrap();
                            cstream.flush().unwrap();
                            //println!("Sent Chunk");
                        }

                        let dno = cstream.read(&mut resp).unwrap();
                        let mut metadata: Value = serde_json::from_slice(&resp[0..dno]).unwrap();
                        metadata["index"] = json!(index);
                        dup2_meta_tx
                            .send((metadata.to_string(), nextserver_ip))
                            .unwrap();
                    });
                    t_handles.push(storage_client_thread);
                }
            }
        }
    });

    // TODO implement size limut
    // Read file content
    let mut total = 0;
    let mut index = 0;
    let mut tempbuffer: Vec<u8> = Vec::new();
    loop {
        let dno = stream.read(&mut destbuffer).unwrap();
        total += dno;
        tempbuffer.append(&mut destbuffer[0..dno].to_vec());
        if index == 1 {
            println!("{:?}", tempbuffer);
        }
        if total % 65536 == 0 {
            let ip = node_array[index % node_array.len()].to_owned();
            sct_tx.send((index, ip, tempbuffer.clone())).unwrap();
            index += 1;
            tempbuffer.clear();
        }

        if total == filesize as usize {
            let ip = node_array[index % node_array.len()].to_owned();
            sct_tx.send((index, ip, tempbuffer.clone())).unwrap();
            tempbuffer.clear();
            sct_tx
                .send((index, String::from("End"), tempbuffer))
                .unwrap();
            break;
        }
    }

    let ndata: String = meta_thread.join().unwrap();
    let client = redis::Client::open("redis://172.28.5.3/7").unwrap();
    let mut con = client.get_connection().unwrap();

    // TODO calculate the hash of the file using the filename and the user UUID
    let user_file_uuid = Uuid::new_v4().to_string();
    let _: () = con.set(&user_file_uuid, ndata).unwrap();
    // TODO
    // Send the files to the node and send the details of each chunk and it's respective node to the core_server
    // Respond back to the user
    Ok(user_file_uuid)
}
