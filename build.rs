fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "net")]
    {
        tonic_build::configure()
            .protoc_arg("--experimental_allow_proto3_optional")
            .compile(&["src/proto/kipdb.proto"], &["src/proto"])?;
    }
    Ok(())
}
