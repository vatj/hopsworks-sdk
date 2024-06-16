pub struct ArrowFlightReadOptions {
    pub(crate) _host: String,
    pub(crate) _port: u16,
    pub(crate) _tls: bool,
    pub(crate) _root_cert_path: Option<String>,
    pub(crate) _token: Option<String>,
}

impl ArrowFlightReadOptions {
    pub fn new(host: &str, port: u16, tls: bool, root_cert_path: Option<&str>, token: Option<&str>) -> Self {
        Self {
            _host: host.to_string(),
            _port: port,
            _tls: tls,
            _root_cert_path: root_cert_path.map(|s| s.to_string()),
            _token: token.map(|s| s.to_string()),
        }
    }
}