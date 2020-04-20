#[macro_use]
extern crate dotenv;
extern crate log;
extern crate redis;
extern crate uuid;

mod api;
mod message;
mod services;

use dotenv::dotenv;
use log::info;
use serde_json::{json, Result, Value};
use std::env;
use std::sync::mpsc;
use std::thread;

use crate::message::{Message, NodeMessage, NodeMsgType};
use api::{forward_to, server_api_main};

//TODO Change to a local variable
static mut mode: u8 = 0;

fn main() -> () {
    dotenv().ok();
    env_logger::init();
    println!("\x1B[H\x1B[2J");
    let proxy_server_uuid = env::var("SERVER_UUID")
        .expect("SERVER_UUID not set")
        .as_str()
        .to_string();

    let mut destbuffer = [0 as u8; 512];
    let reg_data = Message::Node(NodeMessage {
        uuid: proxy_server_uuid,
        msg_type: NodeMsgType::PROXY_REGISTRATION,
        content: json!({
            "request" : "REGISTER",
        })
        .to_string(),
    });

    //let coreserver_ip = String::from("192.168.43.235:7778");
    let coreserver_ip = String::from("127.0.0.1:7778");
    let msg = serde_json::to_string(&reg_data).unwrap();
    loop {
        let dno = forward_to(
            coreserver_ip.clone(),
            msg.as_bytes(),
            &mut destbuffer,
            &String::from("None").clone(),
        );
        if dno == 0{
            return;
        }
        let resp: Value = serde_json::from_slice(&destbuffer[0..dno]).unwrap();
        if resp["response"] == "OK" {
            unsafe {
                mode = resp["mode"].as_u64().unwrap() as u8;
                mode = 0;
            }
            info!("Proxy Server Registration Successfull");
            break;
        }
    }
    let (server_tx, server_rx) = mpsc::channel();
    let _server_thread = thread::spawn(move || {
        server_api_main(server_tx);
    });

    loop {
        let received = server_rx.try_recv();
        match received {
            Ok(s) => {
                info!("Received from Node Client: {}", &s);
                //let ip = s; // Get Node client IP address from the core server api
                //let addr = format!("{}:7777", ip);
                //client_tx.send(addr).unwrap();
            }
            Err(_) => (),
        };
        //  break;
    }
    //_client_thread.join().unwrap();
    //_server_thread.join().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
}
