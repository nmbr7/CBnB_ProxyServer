use crate::api::{forward_to, respond_back};
use crate::message::{Message, ServiceMessage, ServiceMsgType, ServiceType};
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

static HELLO_WORLD: &str = "proxy_uuid";

pub fn query_storage(stream: &mut TcpStream, msg: Value) {
    let proxy_server_uuid = HELLO_WORLD.to_string();

    // TODO Fetch details of the file from the core_server
    let client = redis::Client::open("redis://172.28.5.3/7").unwrap();
    let mut con = client.get_connection().unwrap();

    // TODO calculate the hash of the file using the filename and the user UUID
    let user_uuid = msg["id"].as_str().unwrap().to_string();
    let query = msg["queryname"].as_str().unwrap();
    let files: String = con.get(&user_uuid).unwrap();
    println!("{}", files);
    let mut filemap: HashMap<String, HashMap<String, Value>> =
        serde_json::from_str(&files.as_str()).unwrap();
    match query {
        "ls" => {
            for (k, v) in &mut filemap {
                v.remove("chunk_id");
            }
            stream
                .write_all(json!(filemap).to_string().as_bytes())
                .unwrap();
            stream.flush().unwrap();
        }
        _ => (),
    }
}
pub fn read_storage(stream: &mut TcpStream, msg: Value) {
    let proxy_server_uuid = HELLO_WORLD.to_string();

    // TODO Fetch details of the file from the core_server
    let client = redis::Client::open("redis://172.28.5.3/7").unwrap();
    let mut con = client.get_connection().unwrap();

    // TODO calculate the hash of the file using the filename and the user UUID
    let user_uuid = msg["id"].as_str().unwrap().to_string();
    let filename = msg["filename"].as_str().unwrap().to_string();
    let files: String = con.get(&user_uuid).unwrap();
    let filemap: HashMap<String, HashMap<String, Value>> =
        serde_json::from_str(&files.as_str()).unwrap();
    /*
      println!("{:?}",filemap);
      println!("{}",filemap[&filename]["chunk_id"]);
      println!("{:?\n}",filename);
    */
    let filesize = &filemap[&filename]["size"];
    let chunks: String = con
        .get(&filemap[&filename]["chunk_id"].as_str().unwrap().to_string())
        .unwrap();
    let chunkmap: HashMap<String, Vec<String>> = serde_json::from_str(&chunks.as_str()).unwrap();
    let (sct_tx, sct_rx) = mpsc::channel();

    //println!("{:?}",chunkmap);
    let mut t_handles: Vec<thread::JoinHandle<_>> = vec![];
    for (k, v) in &chunkmap {
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
            uuid: proxy_server_uuid.clone(),
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
    stream
        .write_all(
            json!({
                "total_size": filesize,
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
    let proxy_server_uuid = HELLO_WORLD.to_string();
    println!("Writing file");
    let filesize = msg["filesize"].as_u64().unwrap();
    let filename = msg["filename"].as_str().unwrap().to_string();
    let user_uuid = msg["id"].as_str().unwrap().to_string();

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
        json!(chunklist).to_string()
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

                    let proxy_server_uuid = proxy_server_uuid.clone();
                    let storage_client_thread = thread::spawn(move || {
                        let mut destbuffer = [0 as u8; 512];

                        let content = json!({
                        "msg_type" :  "write",
                        "size"     :   datachunk.len(),
                         })
                        .to_string();

                        let data = ServiceMessage {
                            uuid: proxy_server_uuid.clone(),
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

    let chunkdata: String = meta_thread.join().unwrap();
    let file_uuid = Uuid::new_v4().to_string();

    // TODO Get uuid from the user message

    let client = redis::Client::open("redis://172.28.5.3/7").unwrap();
    let mut con = client.get_connection().unwrap();

    let filedata: String = match con.get(&user_uuid) {
        Ok(val) => val,
        _ => {
            let _: () = con.set(&user_uuid, json!({}).to_string()).unwrap();
            json!({}).to_string()
        }
    };
    let mut filemap: Value = serde_json::from_str(&filedata.as_str()).unwrap();

    // Inserting new file
    filemap[&filename] = json!({
        "chunk_id"  : file_uuid,
        "size": filesize,
        "creation_date": "date",
    });
    println!("{:?} - {:?}", file_uuid, chunkdata);
    let _: () = con.set(&user_uuid, filemap.to_string()).unwrap();
    let _: () = con.set(&file_uuid, chunkdata).unwrap();

    // TODO
    // send the details of each chunk and it's respective node to the core_server
    Ok("Upload Complete".to_string())
}

// FaaS Related Functions

pub fn faas_client_handler(stream: &mut TcpStream) {
    let proxy_server_uuid = HELLO_WORLD.to_string();
    let mut buffer = [0; 55512];
    let no = stream.read(&mut buffer).unwrap();

    let mut data = std::str::from_utf8(&buffer[0..no]).unwrap();
    if data.contains("\r\n\r\n") {
        data = data.split("\r\n\r\n").collect::<Vec<&str>>()[1];
    } else {
        stream
            .write_all(format!("HTTP/1.1 404 Not Found").as_bytes())
            .unwrap();
        stream.flush();
    }
    let req: Value = serde_json::from_str(data).unwrap();

    let client = redis::Client::open("redis://172.28.5.3/4").unwrap();
    let mut con = client.get_connection().unwrap();

    let faasjson: String = match con.get(&req["uuid"].as_str().unwrap().to_string()) {
        Ok(val) => val,
        _ => {
            stream
                .write_all(format!("HTTP/1.1 404 Not Found").as_bytes())
                .unwrap();
            stream.flush();
            return;
        }
    };
    let mut faasmap: Value = serde_json::from_str(&faasjson.as_str()).unwrap();
    let faas_data: Vec<Value> = faasmap[req["faas_uuid"].as_str().unwrap()]
        .as_array()
        .unwrap()
        .to_vec();

    //TODO Check that the ip address doesn't contain port
    let mut t_handles: Vec<thread::JoinHandle<_>> = vec![];
    for faas in faas_data {
        let proxy_uuid = proxy_server_uuid.clone();
        let params: Vec<Value> = req["params"].as_array().unwrap().clone();
        let t_handle = thread::spawn(move || {
            let faas_msg = json!({
                "msg_type":"INVOKE",
                "params":params,
                "id":faas[1],
            })
            .to_string();
            let data = ServiceMessage {
                uuid: proxy_uuid.clone(),
                msg_type: ServiceMsgType::SERVICEINIT,
                service_type: ServiceType::Faas,
                content: faas_msg,
            };
            let msg = serde_json::to_string(&data).unwrap();
            let mut destbuffer = [0 as u8; 512];
            let dno = forward_to(
                format!("{}:7777", faas[0]),
                msg.as_bytes(),
                &mut destbuffer,
                &String::from(""),
            );
            std::str::from_utf8(&destbuffer[0..dno])
                .unwrap()
                .to_string()
        });
        t_handles.push(t_handle);
    }
    let mut resp_data: Vec<String> = vec![];
    for handle in t_handles {
        resp_data.push(handle.join().unwrap());
    }
    //TODO Check the response and send it to the user
    respond_back(stream, resp_data[0].as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run() {
        let mut filename = json!({
            "filename": {
                "id"  : "file_uuid",
                "size": "filesize",
                "creation_date": "date",
            },});
        println!("{}", filename);
        filename["newfile"] = json!({
            "id": "newid",
        });
        println!("{}", filename);
    }
}
