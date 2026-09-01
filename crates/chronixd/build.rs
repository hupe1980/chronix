fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);

    // Chronix gRPC service
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("chronix_descriptor.bin"))
        .compile_protos(&["proto/chronix.proto"], &["proto"])?;

    // Prometheus remote write/read protobuf types (no gRPC service — HTTP only)
    prost_build::Config::new().compile_protos(&["proto/prometheus.proto"], &["proto"])?;

    // OpenTelemetry OTLP metrics protobuf types (HTTP only)
    prost_build::Config::new().compile_protos(&["proto/otlp.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/chronix.proto");
    println!("cargo:rerun-if-changed=proto/prometheus.proto");
    println!("cargo:rerun-if-changed=proto/otlp.proto");
    Ok(())
}
