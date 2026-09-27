use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a vendored protoc so builds don't depend on the host having one installed.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: build scripts are single threaded at this point.
    unsafe { std::env::set_var("PROTOC", protoc) };

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let file = root.join("orderflow/v1/orderflow.proto");
    println!("cargo:rerun-if-changed={}", file.display());

    tonic_prost_build::configure().compile_protos(&[file], &[root])?;
    Ok(())
}
