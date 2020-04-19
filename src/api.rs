use log::info;
use redis::Commands;
use serde_json::{json, Result, Value};
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::TcpListener;
use std::net::TcpStream;
use std::str;
use std::sync::mpsc;
use std::thread;
use uuid::Uuid;

use super::mode;

use crate::message::{
    Message, NodeMessage, NodeMsgType, ServiceMessage, ServiceMsgType, ServiceType,
};

use crate::services::{faas_client_handler, query_storage, read_storage, write_storage};

static HELLO_WORLD: &str = "proxy_uuid";

fn server_api_handler(
    stream: &mut TcpStream,
    server_dup_tx: mpsc::Sender<String>,
    data: (String),
) -> () {
    //let coreserver_ip = String::from("172.28.5.1:7778");
    let coreserver_ip = String::from("127.0.0.1:7778");

    let proxy_server_uuid = HELLO_WORLD.to_string();
    info!("Received connection from {}", &data);

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
        // TODO Define all the client requests
        //      FaaS function invokation and the paas ssh proxy

        // TODO Check the path and match accordingly
        faas_client_handler(stream);
    } else {
        let mut buffer = [0; 100_000];
        let no = stream.read(&mut buffer).unwrap();
        let recv_data: Message = serde_json::from_slice(&buffer[0..no]).unwrap();
        //println!("{}", recv_data);

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
                        //println!("{}", service.content);
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
                        // println!("{:?}", node_array);

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

                        let client = redis::Client::open("redis://172.28.5.3/4").unwrap();
                        let mut con = client.get_connection().unwrap();

                        let user_uuid = service.uuid;
                        let faasdata: String = match con.get(&user_uuid) {
                            Ok(val) => val,
                            _ => {
                                let _: () = con.set(&user_uuid, json!({}).to_string()).unwrap();
                                json!({}).to_string()
                            }
                        };
                        let mut faasmap: Value = serde_json::from_str(&faasdata.as_str()).unwrap();

                        // Inserting new faas data
                        let faas_uuid = Uuid::new_v4().to_string();

                        faasmap[&faas_uuid] = json!(nodedata);

                        let _: () = con.set(&user_uuid, faasmap.to_string()).unwrap();

                        respond_back(stream, &faas_uuid.as_bytes());
                    }
                    ServiceType::Storage => {
                        let msg: Value = serde_json::from_str(&service.content).unwrap();

                        println!("{}", msg);

                        match msg["msg_type"].as_str().unwrap() {
                            "query" => {
                                query_storage(stream, msg);
                            }

                            "read" => {
                                read_storage(stream, msg);
                            }

                            "write" => {
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

                                let core_msg = serde_json::to_string(&faasdata).unwrap();
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
                    ServiceType::Paas => {}
                },
                ServiceMsgType::SERVICEUPDATE => match service.service_type {
                    ServiceType::Faas => {
                        let mut destbuffer = [0 as u8; 512];
                        let faasdata: Value =
                            serde_json::from_str(&service.content.as_str()).unwrap();
                        // Get uuid from the user
                        let faas_uuid = faasdata["id"].as_str().unwrap().to_string();

                        let client = redis::Client::open("redis://172.28.5.3/4").unwrap();
                        let mut con = client.get_connection().unwrap();

                        // Get the nodedata from the redis store (IP addresses and the faas UUID)
                        let nodedata: String = con.get(&faas_uuid).unwrap();

                        let ndata: Value = serde_json::from_str(&nodedata.as_str()).unwrap();
                        let ndataarray = ndata["data"].as_array().unwrap();
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

                ServiceMsgType::SERVICESTART => {}
                ServiceMsgType::SERVICESTOP => {}
            },
        };
    }
}

pub fn forward_to(ip: String, buffer: &[u8], destbuffer: &mut [u8; 512], sip: &String) -> usize {
    info!("Forwarding connection to IP : {}", &ip);
    let mut deststream = TcpStream::connect(&ip).unwrap();

    if ip.ends_with("7778") {
        deststream.write(sip.as_bytes()).unwrap();
        deststream.read(destbuffer).unwrap();
    }
    deststream.write_all(&buffer).unwrap();
    deststream.flush().unwrap();

    deststream.read(destbuffer).unwrap()
}

pub fn respond_back(stream: &mut TcpStream, destbuffer: &[u8]) -> () {
    info!(
        "Responding back to IP : {}\n",
        &stream.peer_addr().unwrap().to_string()
    );
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
