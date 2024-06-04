pub struct ArrowFlightReadOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) tls: bool,
    pub(crate) root_cert_path: Option<String>,
    pub(crate) token: Option<String>,
}