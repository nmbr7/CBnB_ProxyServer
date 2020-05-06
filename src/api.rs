use super::mode;
use chrono::{NaiveDateTime, Utc};
use log::{debug,info};
use redis::Commands;
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::prelude::*;
use std::io::BufReader;
use std::net::TcpListener;
use std::net::TcpStream;
use std::str;
use std::sync::mpsc;
use std::thread;
use uuid::Uuid;

use crate::message::{
    Message, NodeMessage, NodeMsgType, ServiceMessage, ServiceMsgType, ServiceType,
};
use crate::services::http::Http;

use crate::services::{
    faas_client_handler, kvstore_client_handler, paas_main, query_storage, read_storage,
    write_storage,
};

static HELLO_WORLD: &str = "proxy_uuid";

fn server_api_handler(
    stream: &mut TcpStream,
    server_dup_tx: mpsc::Sender<String>,
    data: (String),
) -> () {
    //let coreserver_ip = String::from("172.28.5.1:7778");
    let coreserver_ip = String::from("127.0.0.1:7778");
    //let coreserver_ip = String::from("192.168.43.235:7778");

    let proxy_server_uuid = HELLO_WORLD.to_string();
    info!("Received connection from [{}]", &data);

    if data == coreserver_ip {
        let mut buffer = [0; 512];
        let no = stream.read(&mut buffer).unwrap();
        let resp: Value = serde_json::from_slice(&buffer[0..no]).unwrap();
        if resp["uuid"].as_str().unwrap() == "coreserver_uuid" {
            unsafe {
                mode = resp["mode"].as_u64().unwrap() as u8;
            }
        }
    }
    if unsafe { mode == 1 } {
        let http_content = match Http::parse(stream, 0) {
            Ok(val) => val,
            _ => {
                info!("[invalid request]");
                stream
                    .write_all(format!("HTTP/1.1 404 Not Found\r\n\r\n").as_bytes())
                    .unwrap();
                stream.flush();
                return;
            }
        };
        let data = http_content.body.clone();
        let method = http_content.method.clone();
        let host = http_content.header["Host"].clone();
        let mut path = http_content.path.split("/").collect::<Vec<&str>>();
        path.remove(0);
        /*info!(
            "\nMethod: {}\nHost: {}\nPath: {:?}\nBody: {}",
            method, host, path, data
        );*/
        // http routing based on the host name
        match host.as_str().trim_end_matches(".cbnb.com") {
            "faas" => match path[0] {
                "invoke" => {
                    // Check the id here itself and respond accordingly
                    Http::parse(stream, 1).unwrap();
                    faas_client_handler(stream, data);
                }
                _ => {}
            },
            "kv" => {
                Http::parse(stream, 1).unwrap();
                kvstore_client_handler(stream, data, path, method);
            }
            subdomain => {
                paas_main(stream, &http_content, subdomain.to_string());
                /*
                let subs = subdomain.split(".").collect::<Vec<&str>>();
                    "paas" => paas_main(stream, &http_content, subs[0].to_string()),
                match subs[1]{
                    _=>{
                info!("[invalid request]");
                stream
                    .write_all(format!("HTTP/1.1 404 Not Found\r\n\r\n").as_bytes())
                    .unwrap();
                stream.flush();
                return;
                    }
                }*/
            }
            _ => {
                info!("[invalid request]");
                stream
                    .write_all(format!("HTTP/1.1 404 Not Found\r\n\r\n").as_bytes())
                    .unwrap();
                stream.flush();
                return;
            }
        }
    } else {
        let mut buffer = [0; 100_000];
        let no = stream.read(&mut buffer).unwrap();
        let recv_data: Message = serde_json::from_slice(&buffer[0..no]).unwrap();
        //debug!("{:?}", recv_data);

        match recv_data {
            Message::Node(node) => match node.msg_type {
                NodeMsgType::PROXY_REGISTRATION => {}
                NodeMsgType::REGISTER => {
                    //TODO map the ip to the ip of the coreserver
                    let mut destbuffer = [0 as u8; 512];
                    let dno = forward_to(coreserver_ip, &buffer[0..no], &mut destbuffer, &data);
                    respond_back(stream, &destbuffer[0..dno]);
                }
                NodeMsgType::UPDATE_SYSTAT => {
                    let mut destbuffer = [0 as u8; 512];
                    let dno = forward_to(coreserver_ip, &buffer[0..no], &mut destbuffer, &data);
                    respond_back(stream, &destbuffer[0..dno]);
                }
            },
            Message::Service(service) => match service.msg_type {
                ServiceMsgType::SERVICEINIT => match service.service_type {
                    ServiceType::Faas => {
                        //info!("{}", service.content);
                        let faasco: Value =
                            serde_json::from_str(&service.content.as_str()).unwrap();
                        // Get uuid from the user
                        debug!("{:?}", faasco);
                        let faas_proto = faasco["prototype"].as_str().unwrap().to_string();
                        let mut destbuffer = [0 as u8; 512];

                        let faasdata = Message::Service(ServiceMessage {
                            uuid: proxy_server_uuid,
                            msg_type: ServiceMsgType::SERVICEINIT,
                            service_type: ServiceType::Faas,
                            content: json!({
                                "request" : "select_node",
                            })
                            .to_string(),
                        });

                        let msg = serde_json::to_string(&faasdata).unwrap();
                        let dno = forward_to(coreserver_ip, msg.as_bytes(), &mut destbuffer, &data);
                        let resp: Value = serde_json::from_slice(&destbuffer[0..dno]).unwrap();
                        let alloc_nodes = resp["response"]["node_ip"].as_array().unwrap();

                        let mut node_array: Vec<String> = Vec::new();
                        let mut nodedata: Vec<Vec<String>> = Vec::new();

                        // Store the node ips to a
                        for i in alloc_nodes.iter() {
                            node_array.push(
                                i.as_str().unwrap().split(":").collect::<Vec<&str>>()[0]
                                    .to_string(),
                            );
                        }
                        // info!("{:?}", node_array);
                        let client = redis::Client::open("redis://172.28.5.3/4").unwrap();
                        let mut con = client.get_connection().unwrap();

                        let user_uuid = &service.uuid;
                        let faasdata: String = match con.get(user_uuid) {
                            Ok(val) => val,
                            _ => {
                                let _: () = con
                                    .set(
                                        user_uuid,
                                        json!({
                                            "faass": [" "," "],
                                        })
                                        .to_string(),
                                    )
                                    .unwrap();
                                json!({
                                    "faass": [" "," "],
                                })
                                .to_string()
                            }
                        };
                        let mut faasmap: Value = serde_json::from_str(&faasdata.as_str()).unwrap();
                        let mut faas_array: Vec<String> = Vec::new();

                        // Inserting new faas data
                        debug!("{:?}", faasmap);
                        let faass = faasmap["faass"].as_array().unwrap();
                        for i in faass.iter() {
                            faas_array.push(
                                i.as_str().unwrap().split(":").collect::<Vec<&str>>()[0]
                                    .to_string(),
                            );
                        }
                        debug!("{:?}\n Faas_proto {:?} ", faas_array, faas_proto);
                        for i in &faas_array {
                            let mut hasher = DefaultHasher::new();
                            faas_proto.hash(&mut hasher);
                            let faashash = hasher.finish();
                            debug!("{} == {}", i, faas_proto);
                            if i == &faashash.to_string() {
                                respond_back(stream, format!("Function already exists").as_bytes());
                                return;
                            }
                        }
                        let mut hasher = DefaultHasher::new();
                        faas_proto.hash(&mut hasher);
                        let faashash = hasher.finish();
                        faas_array.push(faashash.to_string());

                        // Forward the request to the node server and get the faas uuid and store the ip and the uuid as a vec of tuple
                        for i in node_array {
                            let nextserver_ip = format!("{}:7777", &i);
                            let dno = forward_to(
                                nextserver_ip.clone(),
                                serde_json::to_string(&service).unwrap().as_bytes(),
                                &mut destbuffer,
                                &data,
                            );
                            nodedata.push(vec![
                                nextserver_ip.to_string(),
                                str::from_utf8(&destbuffer[0..dno]).unwrap().to_string(),
                            ])
                        }

                        faasmap["faass"] = json!(faas_array);

                        let faas_uuid = Uuid::new_v4().to_string();
                        faasmap[&faas_uuid] = json!({ "nodes": nodedata });
                        debug!("Writing FaasMap {:?}", faasmap);
                        let _: () = con.set(user_uuid, faasmap.to_string()).unwrap();

                        respond_back(stream, &faas_uuid.as_bytes());
                    }
                    ServiceType::Storage => {
                        let msg: Value = serde_json::from_str(&service.content).unwrap();

                        debug!("{}", msg);

                        match msg["msg_type"].as_str().unwrap() {
                            "query" => {
                                query_storage(stream, msg);
                            }

                            "read" => {
                                read_storage(stream, msg);
                            }

                            "write" => {
                                let mut destbuffer = [0 as u8; 512];
                                let storagedata = Message::Service(ServiceMessage {
                                    uuid: proxy_server_uuid,
                                    msg_type: ServiceMsgType::SERVICEINIT,
                                    service_type: ServiceType::Storage,
                                    content: json!({
                                        "request" : "select_node",
                                    })
                                    .to_string(),
                                });

                                let core_msg = serde_json::to_string(&storagedata).unwrap();
                                // Fetch list of suitable nodes from the core_server
                                let dno = forward_to(
                                    coreserver_ip,
                                    core_msg.as_bytes(),
                                    &mut destbuffer,
                                    &data,
                                );
                                let core_response: Value =
                                    serde_json::from_slice(&destbuffer[0..dno]).unwrap();

                                let user_file_uuid =
                                    write_storage(stream, msg, core_response).unwrap();
                                respond_back(stream, user_file_uuid.as_bytes());
                            }
                            _ => {}
                        }
                    }
                    ServiceType::Paas => {
                        let msg: Value = serde_json::from_str(&service.content).unwrap();
                        match msg["msg_type"].as_str().unwrap() {
                            "deploy" => {
                                // Fetch list of suitable nodes from the core_server
                                let mut destbuffer = [0 as u8; 512];

                                //let parse_from_str = NaiveDateTime::parse_from_str;
                                //let a = parse_from_str(&utc, "%s").unwrap();
                                let timestamp = Utc::now().timestamp().to_string();
                                let filename = format!("_capp_{}.zip", timestamp);
                                let fname = json!({
                                    "filename" : filename,
                                })
                                .to_string();
                                stream.write_all(fname.as_bytes()).unwrap();
                                stream.flush().unwrap();

                                let paasnode = Message::Service(ServiceMessage {
                                    uuid: proxy_server_uuid.clone(),
                                    msg_type: ServiceMsgType::SERVICEINIT,
                                    service_type: ServiceType::Paas,
                                    content: json!({
                                        "request" : "select_node",
                                    })
                                    .to_string(),
                                });

                                let msg_cont = serde_json::to_string(&paasnode).unwrap();
                                let dno = forward_to(
                                    coreserver_ip,
                                    msg_cont.as_bytes(),
                                    &mut destbuffer,
                                    &data,
                                );
                                let resp: Value =
                                    serde_json::from_slice(&destbuffer[0..dno]).unwrap();
                                let alloc_nodes = resp["response"]["node_ip"].as_array().unwrap();

                                let mut node_array: Vec<String> = Vec::new();
                                let mut nodedata: Vec<Vec<String>> = Vec::new();

                                // Store the node ips to a
                                for i in alloc_nodes.iter() {
                                    node_array.push(
                                        i.as_str().unwrap().split(":").collect::<Vec<&str>>()[0]
                                            .to_string(),
                                    );
                                }

                                let user_uuid = service.uuid.clone();
                                let timestamp = Utc::now().timestamp().to_string();
                                let mut hasher = DefaultHasher::new();
                                format!("{}_{}", timestamp, user_uuid).hash(&mut hasher);
                                let apphash = format!("app-{}", hasher.finish());

                                //***********************************************************
                                // Data to be forwarded to the nodes
                                // TODO Currently the fileid is same as the userid
                                // TODO the storage function needs to implement a wrapper
                                // TODO to hide the real uid and only give access to cloud apps storage
                                let paasdata = ServiceMessage {
                                    uuid: proxy_server_uuid.clone(),
                                    msg_type: ServiceMsgType::SERVICEINIT,
                                    service_type: ServiceType::Paas,
                                    content: json!({
                                    "msg_type": "deploy",
                                    "runtime": msg["runtime"],
                                    "filename": filename,
                                    "fileid": service.uuid,
                                    "tag":apphash,
                                    })
                                    .to_string(),
                                };
                                let no = stream.read(&mut destbuffer).unwrap();
                                let mut status: Value =
                                    serde_json::from_slice(&destbuffer[0..no]).unwrap();
                                match status["UploadStatus"].as_str().unwrap() {
                                    "OK" => {
                                        debug!("Upload Complete");
                                    }
                                    _ => {
                                        respond_back(
                                            stream,
                                            format!("App deployed Failed").as_bytes(),
                                        );
                                        return;
                                    }
                                }

                                let node_msg = serde_json::to_string(&paasdata).unwrap();
                                let mut cnt = 0;
                                for i in node_array {
                                    if cnt > 3 {
                                        break;
                                    }
                                    cnt += 1;
                                    let nextserver_ip = format!("{}:7777", &i);
                                    let dno = forward_to(
                                        nextserver_ip.clone(),
                                        node_msg.as_bytes(),
                                        &mut destbuffer,
                                        &data,
                                    );
                                    nodedata.push(vec![
                                        nextserver_ip.to_string(),
                                        str::from_utf8(&destbuffer[0..dno]).unwrap().to_string(),
                                    ])
                                }
                                let client = redis::Client::open("redis://172.28.5.3/9").unwrap();
                                let mut con = client.get_connection().unwrap();

                                let paasdata: String = match con.get(&user_uuid) {
                                    Ok(val) => val,
                                    _ => {
                                        let _: () = con
                                            .set(
                                                &user_uuid,
                                                json!({
                                                    "apps" : [],
                                                })
                                                .to_string(),
                                            )
                                            .unwrap();
                                        json!({"apps":[]}).to_string()
                                    }
                                };
                                let mut paasmap: Value =
                                    serde_json::from_str(&paasdata.as_str()).unwrap();

                                let apps = paasmap["apps"].as_array().unwrap();
                                let mut appids: Vec<String> = Vec::new();
                                println!("{}", apps.len());
                                if apps.len() != 0 {
                                    for i in apps {
                                        appids.push(i.as_str().unwrap().to_string());
                                    }
                                }
                                appids.push(apphash.clone());
                                paasmap["apps"] = json!(appids);
                                // Wait for the acknowledgement from the app file upload from the user
                                let _: () = con.set(&apphash, json!(nodedata).to_string()).unwrap();
                                let _: () = con.set(&user_uuid, paasmap.to_string()).unwrap();
                                respond_back(
                                    stream,
                                    format!("App deployed at {}.cbnb.com", apphash).as_bytes(),
                                );
                                // TODO Forward some message to the node to report the file upload
                                // TODO or only send to the node if the upload failed
                            }
                            _ => {}
                        }
                    }
                },
                ServiceMsgType::SERVICEUPDATE => match service.service_type {
                    // TODO Consider refactoo
                    ServiceType::Faas => {
                        let mut destbuffer = [0 as u8; 512];
                        let faasdata: Value =
                            serde_json::from_str(&service.content.as_str()).unwrap();
                        // Get uuid from the user
                        debug!("{:?}", faasdata);
                        let faas_uuid = faasdata["id"].as_str().unwrap().to_string();

                        let client = redis::Client::open("redis://172.28.5.3/4").unwrap();
                        let mut con = client.get_connection().unwrap();

                        // Get the nodedata from the redis store (IP addresses and the faas UUID)
                        let nodedata: String = con.get(&service.uuid).unwrap();

                        let ndata: Value = serde_json::from_str(&nodedata.as_str()).unwrap();
                        let ndataarray = ndata[faas_uuid]["nodes"].as_array().unwrap();
                        // Temp read bytes
                        let mut dno: usize = 0;

                        // TODO Handle requests to multiple redundant faas node for integrity
                        //      Also check if the responses are correct or rather are same
                        for i in ndataarray {
                            let nextserver_ip = &i[0].as_str().unwrap().to_string();
                            let msgcontent = ServiceMessage {
                                uuid: proxy_server_uuid.clone(),
                                msg_type: ServiceMsgType::SERVICEUPDATE,
                                service_type: ServiceType::Faas,
                                content: json!({
                                    "msg_type": "MANAGE",
                                    "action"  : "publish",
                                    "id" : i[1].as_str().unwrap().to_string(),
                                })
                                .to_string(),
                            };
                            dno = forward_to(
                                nextserver_ip.to_string(),
                                serde_json::to_string(&msgcontent).unwrap().as_bytes(),
                                &mut destbuffer,
                                &data,
                            );
                        }
                        respond_back(stream, &destbuffer[0..dno]);
                    }
                    ServiceType::Storage => {}
                    ServiceType::Paas => {}
                },

                ServiceMsgType::SERVICEMANAGE => match service.service_type {
                    //Always check the uuid of the sender 
                    ServiceType::Storage => {}
                    ServiceType::Faas => {}
                    ServiceType::Paas => {
                        let paas_manage_data: Value = serde_json::from_str(&service.content.as_str()).unwrap();
                        debug!("{:?}", paas_manage_data);
                        let node_ip = paas_manage_data["node_ip"].as_str().unwrap().to_string();
                        
                        let mut destbuffer = [0 as u8; 512];
                        let msg =  serde_json::to_string(&service).unwrap().as_bytes().to_owned();
                        let dno = forward_to(node_ip,&msg, &mut destbuffer, &data);
                        respond_back(stream, &destbuffer[0..dno]);
                    }
                }
            },
        };
    }
}
pub fn try_connect(ip: String) -> Result<TcpStream, ()> {
    use std::{thread, time};
    info!("connecting to [{}]", &ip);
    let mut cnt = 0;
    loop {
        match TcpStream::connect(&ip) {
            Ok(val) => return Ok(val),
            _ => {
                if cnt != 5 {
                    let sec = time::Duration::from_secs(2);
                    thread::sleep(sec);
                    info!("Retrying");
                    cnt += 1;
                    continue;
                } else {
                    info!("Drop connection attempt");
                    // TODO Handler all the return at the caller properly
                    return Err(());
                }
            }
        };
    }
}

pub fn forward_to(ip: String, buffer: &[u8], destbuffer: &mut [u8; 512], sip: &String) -> usize {
    info!("Forwarding connection to [{}]", &ip);
    let mut deststream = try_connect(ip.clone()).unwrap();
    if ip.ends_with("7778") {
        deststream.write(sip.as_bytes()).unwrap();
        deststream.read(destbuffer).unwrap();
    }
    deststream.write_all(&buffer).unwrap();
    deststream.flush().unwrap();
    return deststream.read(destbuffer).unwrap();
}

pub fn respond_back(stream: &mut TcpStream, destbuffer: &[u8]) -> () {
    /*info!(
        "Responding back to IP : {}\n",
        &stream.peer_addr().unwrap().to_string()
    );*/
    stream.write_all(&destbuffer);
    stream.flush().unwrap();
}
/*
fn read_and_forward(
    readstream: &mut TcpStream,
    writestream: &mut TcpStream,
    readsize: usize,
    writefull: bool,
) -> std::result::Result<String, ()> {
    // Read file content
    let mut destbuffer = [0 as u8; 2048];
    let mut total = 0;
    let mut index = 0;
    let mut tempbuffer: Vec<u8> = Vec::new();
    loop {
        let dno = readstream.read(&mut destbuffer).unwrap();
        total += dno;
        tempbuffer.append(&mut destbuffer[0..dno].to_vec());
        if total % 65536 == 0 {
            writestream.write_all(tempbuffer.as_slice()).unwrap();
            writestream.flush().unwrap();
            tempbuffer.clear();
        }

        if total == readsize {
            writestream.write_all(tempbuffer.as_slice()).unwrap();
            writestream.flush().unwrap();
            tempbuffer.clear();
            break;
        }
    }
    Ok("OK".to_string())
}
*/

pub fn server_api_main(server_tx: mpsc::Sender<String>) -> () {
    let listener = TcpListener::bind("0.0.0.0:7779").unwrap();
    info!("Waiting for proxy connections");
    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        let data = (stream.peer_addr().unwrap().to_string());

        let server_dup_tx = mpsc::Sender::clone(&server_tx);

        thread::spawn(move || {
            server_api_handler(&mut stream, server_dup_tx, data);
        });
    }
}
