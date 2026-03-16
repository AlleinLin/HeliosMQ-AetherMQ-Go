pub mod proto;

use tonic::transport::Server;

pub struct ProtocolServer {
    port: u16,
}
