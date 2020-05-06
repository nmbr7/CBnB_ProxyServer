use serde::{Deserialize, Serialize};
use serde_json::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServiceType {
    // Send to the node
    Storage,
    Faas,
    Paas, // CUSTOM,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServiceMsgType {
    // CHECKSYSTAT,
    SERVICEUPDATE,
    SERVICEINIT,
    SERVICEMANAGE,
    // CUSTOM,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceMessage {
    pub uuid: String,
    pub msg_type: ServiceMsgType,
    pub service_type: ServiceType,
    pub content: String,
}

//////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeMsgType {
    // Received from the node
    REGISTER,
    PROXY_REGISTRATION,
    UPDATE_SYSTAT,
    // Send to the node
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMessage {
    pub uuid: String,
    pub msg_type: NodeMsgType,
    pub content: String, //sys_stat::Resources,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Node(NodeMessage),
    Service(ServiceMessage),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run() {
        //  println!("{:?}",Message::new(MsgType::REGISTER,stat))
        //println!("{}", Message::<sys_stat::NodeResources>::register(stat))
    }
}
