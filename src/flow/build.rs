fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate kura (yoriito VISS) gRPC client from proto definitions.
    // We only build the client stubs — the server side is not needed.
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile(&["proto/yoriito/viss/v1/producer.proto"], &["proto"])?;
    Ok(())
}
