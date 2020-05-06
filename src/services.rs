use crate::api::{forward_to, respond_back, try_connect};
use crate::message::{Message, ServiceMessage, ServiceMsgType, ServiceType};
use log::{debug, info};
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

// Async related
use futures::future::try_join;
use futures::FutureExt;
use std::env;
use std::error::Error;
use tokio::io as tio;
use tokio::net::TcpStream as tokstream;
use tokio::prelude::*;

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
    //debug!("{}", files);
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

    //debug!("\n********************************************************");
    //debug!("{:?}", filemap);
    //debug!("{:?\n}", filename);
    //debug!("{}", filemap[&filename]["chunk_id"]);
    //debug!("\n********************************************************");

    let filesize = &filemap[&filename]["size"];
    let chunks: String = con
        .get(&filemap[&filename]["chunk_id"].as_str().unwrap().to_string())
        .unwrap();
    let chunkmap: HashMap<String, Vec<String>> = serde_json::from_str(&chunks.as_str()).unwrap();
    let (sct_tx, sct_rx) = mpsc::channel();

    //debug!("{:?}", chunkmap);
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
        debug!("{}", content);

        let data = ServiceMessage {
            uuid: proxy_server_uuid.clone(),
            msg_type: ServiceMsgType::SERVICEINIT,
            service_type: ServiceType::Storage,
            content: content,
        };

        let msg_data = serde_json::to_string(&data).unwrap();

        let dup_sct_tx = mpsc::Sender::clone(&sct_tx);
        let storage_client_thread = thread::spawn(move || {
            let mut cstream = try_connect(nextserver_ip.clone()).unwrap();
            cstream.write_all(msg_data.as_bytes()).unwrap();
            cstream.flush().unwrap();

            let mut destbuffer = [0 as u8; 2048];
            let mut total = 0 as usize;
            let mut bufvec: Vec<u8> = vec![];
            loop {
                let dno = cstream.read(&mut destbuffer).unwrap();
                total += dno;
                debug!("Total {} - dno {} - size {}", total, dno, size);
                bufvec.append(&mut destbuffer[0..dno].to_vec());
                if total >= size {
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
        .write_all(json!({ "total_size": filesize }).to_string().as_bytes())
        .unwrap();
    stream.flush().unwrap();
    let mut resp = [0; 512];
    let no = stream.read(&mut resp).unwrap();
    if std::str::from_utf8(&resp[0..no]).unwrap() == "OK" {
        debug!("Send the Total file size");
    }
    for received in sct_rx.try_iter() {
        match received {
            ((size, index), chunk, dat) => {
                let end: &String = &dat;
                if end.trim() == String::from("End") {
                    debug!("Sending {:?}", end.trim());
                    stream
                        .write_all(
                            json!({
                                "msg_type": "End",
                            })
                            .to_string()
                            .as_bytes(),
                        )
                        .unwrap();
                    stream.flush().unwrap();
                    break;
                }
                debug!("Index [{}] - Senting to client chunk metadata", index);
                
                // Write and check response OK 
                stream
                    .write_all(
                        json!({
                            "msg_type": "meta",
                            "size"    : size,
                            "index"   : index,
                        })
                        .to_string()
                        .as_bytes(),
                    )
                    .unwrap();
                stream.flush().unwrap();
                resp = [0; 512];
                let no = stream.read(&mut resp).unwrap();
                if std::str::from_utf8(&resp[0..no]).unwrap() == "OK" {
                    debug!(
                        "Index [{}] - Received OK, Senting to client chunk data  of Size :- {} ",
                        index, size
                    );
                
                   // Write and check response OK 
                    debug!("Chunk Start 10 bytes {:?}",&chunk[0..10]);
                    debug!("Chunk Last  10 bytes {:?}",&chunk[chunk.len()-10..chunk.len()]);
                    stream.write_all(&chunk.as_slice()[0..size]).unwrap();
                    stream.flush().unwrap();
                    resp = [0; 512];
                    let no = stream.read(&mut resp).unwrap();
                    if std::str::from_utf8(&resp[0..no]).unwrap() == "OK" {
                        debug!(
                        "Index [{}] - Response after sending chunk {}",
                        index,
                        std::str::from_utf8(&resp[0..no]).unwrap()
                    );
                    }
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
    debug!("Writing file");
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
        debug!("{}", i);
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
                    //debug!("Received meadata {}", metadata);
                    let end: &String = &metadata;
                    if end.trim() == String::from("End") {
                        debug!("{:?}", end.trim());
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

                            use std::{time};
    let dup_meta_tx = mpsc::Sender::clone(&meta_tx);
    let node_thread = thread::spawn(move || {
        let mut t_handles: Vec<thread::JoinHandle<_>> = vec![];
        for recv in sct_rx.iter() {
            match recv {
                (index, ip, chunkbuf) => {
                    let end: &String = &ip;
                    debug!("Got {:?} after fetching all the files", end.trim());
                    debug!("****** Current Thread count is [ {} ] ****** and index [ {} ]",t_handles.len(),index+1);
                    if end.trim() == String::from("End") {
                        while index+1 != t_handles.len(){
                            println!("Waiting in loop index [{}] , thread cnt [{}]",index+1,t_handles.len());
                            debug!("Waiting in loop index [{}] , thread cnt [{}]",index+1,t_handles.len());
                            let sec = time::Duration::from_secs(2);
                            thread::sleep(sec);
                        }
                        debug!("****** Current Thread count is [ {} ] ******",t_handles.len());
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
                    debug!(
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

                        let mut cstream = try_connect(nextserver_ip.clone()).unwrap();
                        cstream.write_all(msg_data.as_bytes()).unwrap();
                        cstream.flush().unwrap();

                        let no = cstream.read(&mut resp).unwrap();

                        if std::str::from_utf8(&resp[0..no]).unwrap() == "OK" {
                            cstream.write_all(datachunk.as_slice()).unwrap();
                            cstream.flush().unwrap();
                            debug!("Sent Chunk of index [{}] to {:?}",index,nextserver_ip);
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
    let mut destbuffer = [0 as u8; 2048];
    loop {
        let dno = stream.read(&mut destbuffer).unwrap();
        total += dno;
        tempbuffer.append(&mut destbuffer[0..dno].to_vec());
        /*if index == 1 {
            debug!("{:?}", tempbuffer);
        }*/
        //if total % 65536 == 0 {
        if total % 4046848 == 0 {
            //debug!("{}", node_array.len());
            let ip = node_array[index % node_array.len()].to_owned();
            sct_tx.send((index, ip, tempbuffer.clone())).unwrap();
            index += 1;
            tempbuffer.clear();
        }

        if total == filesize as usize {
            let ip = node_array[index % node_array.len()].to_owned();
            sct_tx.send((index, ip, tempbuffer.clone())).unwrap();
            tempbuffer.clear();
            use std::{thread, time};
            let sec = time::Duration::from_secs(2);
            thread::sleep(sec);
            sct_tx
                .send((index, String::from("End"), tempbuffer))
                .unwrap();
            break;
        }
    }

    let chunkdata: String = meta_thread.join().unwrap();
    let file_uuid = Uuid::new_v4().to_string();

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

    //**********************************************************************************************

    // TODO Check if the file already exists
    // TODO Manage the app and key-value store metadate seperately (Implement a seperate structure)

    //**********************************************************************************************

    // Inserting new file
    filemap[&filename] = json!({
        "chunk_id"  : file_uuid,
        "size": filesize,
        "creation_date": "date",
    });
    //debug!("{:?} - {:?}", file_uuid, chunkdata);
    let _: () = con.set(&user_uuid, filemap.to_string()).unwrap();
    let _: () = con.set(&file_uuid, chunkdata).unwrap();
    debug!("Sending back upload complete");

    // TODO
    // send the details of each chunk and it's respective node to the core_server
    Ok("Upload Complete".to_string())
}

pub fn getfromstore(
    cstream: &mut TcpStream,
    data: String,
    addr: String,
    path: Vec<&str>,
    method: String,
) -> String {
    if path.len() != 1 {
        cstream
            .write_all(format!("HTTP/1.1 404 Not Found\r\n\r\n").as_bytes())
            .unwrap();
        cstream.flush();
        return String::from("Error");
    }
    let proxy_server_uuid = HELLO_WORLD.to_string();
    // Currently take only small values \
    // Since reordering and keeping values in memory is expensive
    // TODO Handle reordering of large file chunks

    let recv_data: Value = serde_json::from_str(&data).unwrap();
    let content = json!({
        "msg_type" :  "read",
        "filename" :  path[0],
        "id"       :  recv_data["id"],
    })
    .to_string();

    let data = Message::Service(ServiceMessage {
        msg_type: ServiceMsgType::SERVICEINIT,
        service_type: ServiceType::Storage,
        content: content,
        uuid: proxy_server_uuid,
    });

    let msg_data = serde_json::to_string(&data).unwrap();
    //debug!("{}",test["content"].as_str().unwrap(()));

    let mut resp = [0; 2048];
    let mut destbuffer = [0 as u8; 2048];

    let mut stream = try_connect(addr).unwrap();
    debug!("{:?}", msg_data);
    stream.write_all(msg_data.as_bytes()).unwrap();
    stream.flush().unwrap();
    // TODO Handle the trailing character error

    let no = stream.read(&mut resp).unwrap();
    let fsize: Value = serde_json::from_slice(&resp[0..no]).unwrap();
    let filesize = fsize["total_size"].as_u64().unwrap() as usize;
    if filesize > 4046848 {
        return String::from("OK");
    }
    stream.write_all(String::from("OK").as_bytes()).unwrap();
    stream.flush().unwrap();
    let mut bufvec: Vec<Vec<u8>> = Vec::with_capacity(filesize / 65536);

    let mut totalfilesize = 0 as usize;
    loop {
        let no = stream.read(&mut resp).unwrap();
        stream.write_all(String::from("OK").as_bytes()).unwrap();
        stream.flush().unwrap();
        debug!("val {}", std::str::from_utf8(&resp[0..no]).unwrap());
        let metadata: Value = serde_json::from_slice(&resp[0..no]).unwrap();
        debug!("{}", metadata);
        if metadata["msg_type"].as_str().unwrap() == "End" {
            break;
        }

        let size = metadata["size"].as_u64().unwrap() as usize;
        let index = metadata["index"].as_u64().unwrap();
        let mut total = 0 as usize;
        //let mut bufvec: Vec<u8> = vec![];
        loop {
            let mut dno = stream.read(&mut destbuffer).unwrap();
            if dno > size {
                dno = size;
            }
            total += dno;
            //println!("{:?}",destbuffer[(dno-15)..dno].to_vec());
            bufvec.insert(index as usize, destbuffer[0..dno].to_vec());
            println!("Total: {} - dno: {} - Size {}", total, dno, size);
            if total >= size {
                stream.write_all(String::from("OK").as_bytes()).unwrap();
                stream.flush().unwrap();
                break;
            }
        }

        totalfilesize += total;
        if totalfilesize > 4046848 {
            return String::from("OK");
        }
        let mut yieldcnt = 0;
        let mut id = 0;
        for i in bufvec.clone() {
            if i.is_empty() && yieldcnt > id {
                id += 1;
                continue;
            } else {
                if (id - yieldcnt > 0) || yieldcnt - id < 0 {
                    break;
                }
                if id == 0 {
                    cstream
                        .write_all(format!("HTTP/1.1 200 OK\r\n\r\n").as_bytes())
                        .unwrap();
                }
                cstream.write_all(i.as_slice()).unwrap();
                cstream.flush();
                bufvec[id].clear();
                yieldcnt += 1;
            }
            id += 1;
        }
        if totalfilesize == filesize {
            break;
        }
    }
    return String::from("OK");
}

pub fn uploadtostore(stream: &mut TcpStream, data: String, addr: String) -> String {
    let proxy_server_uuid = HELLO_WORLD.to_string();
    let recv_data: Value = serde_json::from_str(&data).unwrap();
    debug!("{:?}", recv_data);

    let map: HashMap<String, String> =
        serde_json::from_str(json!(recv_data["kv"]).to_string().as_str()).unwrap();

    let (mut key, mut buf) = (String::new(), String::new());
    for (k, v) in map {
        key = k;
        buf = v;
    }
    let content = json!({
        "id"       : recv_data["id"],
        "msg_type" :  "write",
        "filename" :  key,
        "filesize" :  buf.len(),
    })
    .to_string();

    let data = Message::Service(ServiceMessage {
        msg_type: ServiceMsgType::SERVICEINIT,
        service_type: ServiceType::Storage,
        content: content,
        uuid: proxy_server_uuid,
    });
    let msg_data = serde_json::to_string(&data).unwrap();
    //debug!("{}",test["content"].as_str().unwrap(());

    let mut resp = [0; 512];
    let mut stream = try_connect(addr).unwrap();
    debug!("{:?}", msg_data);
    stream.write_all(msg_data.as_bytes()).unwrap();

    let no = stream.read(&mut resp).unwrap();
    if std::str::from_utf8(&resp[0..no]).unwrap() == "OK" {
        stream.write_all(&buf.as_bytes()).unwrap();
        debug!("Sent");
    }

    let no = stream.read(&mut resp).unwrap();
    let mut data = std::str::from_utf8(&resp[0..no]).unwrap();
    debug!("Returned: {}", data);
    key.to_string()
}

#[tokio::main]
pub async fn paas_main(stream: &mut TcpStream, http_data: &http::Http, appid: String) {
    let mut path = http_data.path.split("/").collect::<Vec<&str>>();
    path.remove(0);
    //let app_uuid = path[1];
    // TODO Lookup the node related to the app_uuid from the proxy server or from the cache;
    let tag = appid.to_string();
    
    let client = redis::Client::open("redis://172.28.5.3/9").unwrap();
    let mut con = client.get_connection().unwrap();
    let nodedata: String = con.get(&tag).unwrap();

    let paasjson: String = match con.get(&tag) {
        Ok(val) => val,
        _ => {
            stream
                .write_all(format!("HTTP/1.1 404 Not Found\r\n\r\n").as_bytes())
                .unwrap();
            stream.flush();
            return;
        }
    };
    debug!("{:?}", paasjson);
    let mut paasarray: Value = serde_json::from_str(&paasjson.as_str()).unwrap();



    let paas_data: Vec<Value> = paasarray.as_array().unwrap().to_vec();

    let server_addr = format!("{}:7070",paas_data[0][0].as_str().unwrap().split(":").collect::<Vec<&str>>()[0]);
    let inbound = tokstream::from_std(stream.try_clone().unwrap()).unwrap();
    info!("Got Connection");
    tokio::join!(transfer(inbound, server_addr.clone(), tag));
}

async fn transfer(mut inbound: tokstream, proxy_addr: String, tag: String) {
    let mut b1 = [0; 512];
    //debug!("{:?}",std::str::from_utf8(&b1[0..n]).unwrap());
    info!("Relaying to app: {}", proxy_addr);
    let mut outbound = tokstream::connect(proxy_addr).await.unwrap();
    outbound
        .write(
            json!({
                "msg_type":"invoke",
                "tag":tag,
            })
            .to_string()
            .as_bytes(),
        )
        .await
        .unwrap();
    outbound.read(&mut b1).await.unwrap();

    let (mut ri, mut wi) = inbound.split();
    let (mut ro, mut wo) = outbound.split();

    let client_to_server = tio::copy(&mut ri, &mut wo);
    let server_to_client = tio::copy(&mut ro, &mut wi);

    try_join(client_to_server, server_to_client).await.unwrap();
}

pub fn kvstore_client_handler(
    stream: &mut TcpStream,
    data: String,
    path: Vec<&str>,
    method: String,
) {
    // TODO Fetch the address of the proxy server from the core server
    let addr = String::from("127.0.0.1:7779");
    //let addr = String::from("172.28.5.77:7779");
    match method.as_str() {
        "PUT" => {
            // TODO Fetch the proxy server address
            let kvid = uploadtostore(stream, data, addr);
            let resp = format!("http://cbnb.com/{}", kvid);
            stream
                .write_all(format!("HTTP/1.1 200 OK\r\n\r\n{}", resp.trim()).as_bytes())
                .unwrap();
            stream.flush();
        }
        "GET" => {
            getfromstore(stream, data, addr, path, method);
        }
        "POST" => {
            getfromstore(stream, data, addr, path, method);
        }
        "DELETE" => {}
        _ => (),
    }
}

// FaaS Related Functions

pub mod http {
    use super::*;
    pub struct Http {
        pub method: String,
        pub path: String,
        pub header: HashMap<String, String>,
        pub body: String,
    }

    impl Http {
        pub fn parse(stream: &mut TcpStream, read_mode: u8) -> std::result::Result<Self, String> {
            let mut method: String = String::new();
            let mut path: String = String::new();
            let mut header: HashMap<String, String> = HashMap::new();
            let mut body: String = String::new();

            let mut buffer = [0; 55512];
            let no = if read_mode == 0 {
                stream.peek(&mut buffer).unwrap()
            } else {
                stream.read(&mut buffer).unwrap()
            };
            let mut data = std::str::from_utf8(&buffer[0..no]).unwrap();

            if data.contains("\r\n\r\n") {
                let temp = data.split("\r\n\r\n").collect::<Vec<&str>>()[0];
                body = data.split("\r\n\r\n").collect::<Vec<&str>>()[1].to_string();
                let mut upper = temp.split("\r\n").collect::<Vec<&str>>();
                let l1 = upper[0].split(" ").collect::<Vec<&str>>();
                if l1.len() != 3 {
                    return Err(format!("Invalid Request"));
                } else {
                    method = l1[0].to_string();
                    path = l1[1].to_string();
                }
                upper.remove(0);
                for head in upper {
                    let d = head.split(":").collect::<Vec<&str>>();
                    header.insert(d[0].to_string(), d[1].trim().to_string());
                }
            } else {
                return Err(format!("Invalid Request"));
            }

            Ok(Self {
                method,
                path,
                header,
                body,
            })
        }
    }
}
pub fn faas_client_handler(stream: &mut TcpStream, data: String) {
    let proxy_server_uuid = HELLO_WORLD.to_string();

    let req: Value = match serde_json::from_str(data.as_str()) {
        Ok(val) => val,
        _ => {
            stream
                .write_all(format!("HTTP/1.1 404 Not Found\r\n\r\n").as_bytes())
                .unwrap();
            stream.flush();
            return;
        }
    };

    let client = redis::Client::open("redis://172.28.5.3/4").unwrap();
    let mut con = client.get_connection().unwrap();

    let faasjson: String = match con.get(&req["uuid"].as_str().unwrap().to_string()) {
        Ok(val) => val,
        _ => {
            stream
                .write_all(format!("HTTP/1.1 404 Not Found\r\n\r\n").as_bytes())
                .unwrap();
            stream.flush();
            return;
        }
    };
    debug!("{:?}", faasjson);
    let mut faasmap: Value = serde_json::from_str(&faasjson.as_str()).unwrap();
    let faas_data: Vec<Value> = faasmap[req["faas_uuid"].as_str().unwrap()]["nodes"]
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
                format!(
                    "{}:7777",
                    faas[0].as_str().unwrap().split(":").collect::<Vec<&str>>()[0]
                ),
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
    debug!("Faas Response {:?}", resp_data);
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
        debug!("{}", filename);
        filename["newfile"] = json!({
            "id": "newid",
        });
        debug!("{}", filename);
    }
}
