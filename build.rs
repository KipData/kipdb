fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("src/proto/net.proto")?;
    tonic_build::compile_protos("src/proto/kipdb.proto")?;
    Ok(())
}