use std::io::prelude::*;
use std::io::BufReader;
use std::net::TcpListener;
use std::net::TcpStream;
use std::str;
use std::sync::mpsc;
use std::thread;

use serde_json::{json, Result, Value};

use crate::message::{
    Message, NodeMessage, NodeMsgType, ServiceMessage, ServiceMsgType, ServiceType,
};

fn server_api_handler(
    mut stream: TcpStream,
    server_dup_tx: mpsc::Sender<String>,
    data: (String),
) -> () {
    println!("Received connection from {}", &data);

    let mut buffer = [0; 100_000];
    let no = stream.read(&mut buffer).unwrap();
    let recv_data: Message = serde_json::from_slice(&buffer[0..no]).unwrap();
    //println!("{}", recv_data);

    //let coreserver_ip = String::from("172.28.5.1:7778");
    let coreserver_ip = String::from("127.0.0.1:7778");
    match recv_data {
        Message::Node(node) => match node.msg_type {
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
            ServiceMsgType::SERVICEINIT => {
                match service.service_type {
                    ServiceType::Faas => {
                        //println!("{}", service.content);
                        let mut destbuffer = [0 as u8; 512];

                        let faasdata = Message::Service(ServiceMessage {
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
                        let alloc_nodes =
                            resp["response"]["node_ip"].as_array().unwrap();
                        let nextserver_ip = alloc_nodes[0].as_str().unwrap().to_string();
                        let dno = forward_to(
                            nextserver_ip,
                            serde_json::to_string(&service).unwrap().as_bytes(),
                            &mut destbuffer,
                            &data,
                        );
                        respond_back(stream, &destbuffer[0..dno]);
                    }
                    ServiceType::Storage => {
                        let msg: Value = serde_json::from_str(&service.content).unwrap();
                        match msg["msg_type"].as_str().unwrap() {
                            "read" => {
                                // TODO
                                // Fetch details of the file from the core_server
                                // Request nodes to fetch the file chunks
                                // Combine the encrypted file chunks in correct order and decrypt it using some key
                                // Send the file back to the user in json, also accessible from the website
                            }
                            "write" => {
                                // TODO
                                // Encrypt the file and split it to chunks of equal size
                                // Fetch list of suitable nodes from the core_server
                                // Send the files to the node and send the details of each chunk and it's respective node to the core_server
                                // Respond back to the user
                            }
                            _ => {}
                        }
                    }
                }
            }
            ServiceMsgType::SERVICEUPDATE => match service.service_type {
                ServiceType::Faas => {
                    let mut destbuffer = [0 as u8; 512];
                    let nextserver_ip = String::from("dsada");
                    let dno = forward_to(
                        nextserver_ip,
                        serde_json::to_string(&service).unwrap().as_bytes(),
                        &mut destbuffer,
                        &data,
                    );
                    respond_back(stream, &destbuffer[0..dno]);
                }
                ServiceType::Storage => {}
            },
            ServiceMsgType::SERVICESTART => {}
            ServiceMsgType::SERVICESTOP => {}
        },
    };
}

fn forward_to(ip: String, buffer: &[u8], destbuffer: &mut [u8; 512], sip: &String) -> usize {
    println!("Forwarding connection to IP : {}", &ip);
    let mut deststream = TcpStream::connect(ip).unwrap();
    
    deststream.write(sip.as_bytes()).unwrap();
    deststream.read(destbuffer).unwrap();
    
    deststream.write_all(&buffer).unwrap();
    deststream.flush().unwrap();
    
    deststream.read(destbuffer).unwrap()
}

fn respond_back(mut stream: TcpStream, destbuffer: &[u8]) -> () {
    println!("Responding back to IP : {}\n", &stream.peer_addr().unwrap().to_string());
    stream.write_all(&destbuffer);
    stream.flush().unwrap();
}

pub fn server_api_main(server_tx: mpsc::Sender<String>) -> () {
    let listener = TcpListener::bind("0.0.0.0:7779").unwrap();
    println!("Waiting for proxy connections");
    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        let data = (stream.peer_addr().unwrap().to_string());

        let server_dup_tx = mpsc::Sender::clone(&server_tx);

        thread::spawn(move || {
            server_api_handler(stream, server_dup_tx, data);
        });
    }
}
