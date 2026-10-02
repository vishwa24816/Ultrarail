//! Fixture cert generator: `cargo run --example gen_certs`.
//! Writes a self-signed cert for 127.0.0.1/localhost into tests/certs/.
//! Sandbox fixtures only — never production credentials.

fn main() {
    let cert =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    std::fs::create_dir_all("tests/certs").unwrap();
    std::fs::write("tests/certs/cert.pem", cert.cert.pem()).unwrap();
    std::fs::write("tests/certs/key.pem", cert.key_pair.serialize_pem()).unwrap();
    println!("wrote tests/certs/cert.pem + key.pem");
}
