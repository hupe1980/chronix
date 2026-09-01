fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/data.proto", "proto/region_raft.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/data.proto");
    println!("cargo:rerun-if-changed=proto/region_raft.proto");
    Ok(())
}
