use std::env;
use std::fs;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::os::unix::process::CommandExt;
use std::process::{self, Command};
use std::time::Duration;

const APPLICATION: &str = "/usr/local/bin/invitations";
const SECRET_ENVIRONMENTS: &[(&str, &str)] = &[
    ("DATABASE_URL_FILE", "DATABASE_URL"),
    ("NEWAPI_DATABASE_URL_FILE", "NEWAPI_DATABASE_URL"),
    ("SESSION_SECRET_FILE", "SESSION_SECRET"),
    ("SYNC_SECRET_FILE", "SYNC_SECRET"),
    (
        "INVITATIONS_INTERNAL_SECRET_FILE",
        "INVITATIONS_INTERNAL_SECRET",
    ),
    (
        "REBATE_EVENT_INGEST_SECRET_FILE",
        "REBATE_EVENT_INGEST_SECRET",
    ),
];

fn main() {
    let arguments: Vec<String> = env::args().skip(1).collect();
    match arguments.first().map(String::as_str) {
        Some("--healthcheck") => process::exit(healthcheck()),
        Some("--self-test") => {
            println!("invitations-entrypoint: ok");
            return;
        }
        _ => {}
    }

    if let Err(error) = load_secrets() {
        eprintln!("invitations entrypoint configuration error: {error}");
        process::exit(78);
    }

    let error = Command::new(APPLICATION).args(arguments).exec();
    eprintln!("failed to execute Invitations: {error}");
    process::exit(70);
}

fn load_secrets() -> Result<(), String> {
    for (file_environment, target_environment) in SECRET_ENVIRONMENTS {
        let path = env::var(file_environment)
            .map_err(|_| format!("{file_environment} must be set"))?;
        if !path.starts_with("/run/secrets/") {
            return Err(format!("{file_environment} must reference /run/secrets"));
        }
        let value = read_secret(&path)
            .map_err(|error| format!("cannot read {file_environment}: {error}"))?;
        env::set_var(target_environment, value);
        env::remove_var(file_environment);
    }
    Ok(())
}

fn read_secret(path: &str) -> io::Result<String> {
    let content = fs::read_to_string(path)?;
    let value = content.trim_end_matches(|character| character == '\n' || character == '\r');
    if value.is_empty() || value.contains(['\n', '\r', '\0']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "secret must be a non-empty single line",
        ));
    }
    Ok(value.to_owned())
}

fn healthcheck() -> i32 {
    let listen_address = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:6212".to_owned());
    let Some(port) = listen_port(&listen_address) else {
        return 2;
    };
    let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    match TcpStream::connect_timeout(&address, Duration::from_secs(2)) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

fn listen_port(address: &str) -> Option<u16> {
    address.rsplit_once(':')?.1.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn parses_ipv4_and_ipv6_listen_ports() {
        assert_eq!(listen_port("0.0.0.0:6212"), Some(6212));
        assert_eq!(listen_port("[::]:6212"), Some(6212));
        assert_eq!(listen_port("invalid"), None);
    }

    #[test]
    fn trims_only_the_secret_file_line_ending() {
        let path = temporary_file("secret-line-ending");
        fs::write(&path, "opaque-value\r\n").unwrap();
        assert_eq!(read_secret(path.to_str().unwrap()).unwrap(), "opaque-value");
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_empty_and_multiline_secrets() {
        let empty = temporary_file("empty-secret");
        fs::write(&empty, "\n").unwrap();
        assert!(read_secret(empty.to_str().unwrap()).is_err());
        fs::remove_file(empty).unwrap();

        let multiline = temporary_file("multiline-secret");
        fs::write(&multiline, "line-one\nline-two\n").unwrap();
        assert!(read_secret(multiline.to_str().unwrap()).is_err());
        fs::remove_file(multiline).unwrap();
    }

    fn temporary_file(name: &str) -> PathBuf {
        env::temp_dir().join(format!("kkai-invitations-{name}-{}", process::id()))
    }
}
