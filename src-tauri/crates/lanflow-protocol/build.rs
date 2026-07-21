fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc is unavailable");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.bytes(["."]);
    config
        .compile_protos(&["proto/lanflow.proto"], &["proto"])
        .expect("failed to compile LanFlow protocol");
    println!("cargo:rerun-if-changed=proto/lanflow.proto");
}
