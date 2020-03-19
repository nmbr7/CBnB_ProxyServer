#[macro_use]
extern crate dotenv;
extern crate redis;
extern crate uuid;

mod api;
mod message;

use dotenv::dotenv;
use std::env;
use std::sync::mpsc;
use std::thread;

use api::server_api_main;

fn main() -> () {
    let (server_tx, server_rx) = mpsc::channel();

    let _server_thread = thread::spawn(move || {
        server_api_main(server_tx);
    });

    loop {
        let received = server_rx.try_recv();
        match received {
            Ok(s) => {
                println!("Received from Node Client: {}", &s);
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
