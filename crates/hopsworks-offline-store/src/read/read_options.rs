pub struct ArrowFlightReadOptions {
    pub(crate) _host: String,
    pub(crate) _port: u16,
    pub(crate) _tls: bool,
    pub(crate) _root_cert_path: Option<String>,
    pub(crate) _token: Option<String>,
}