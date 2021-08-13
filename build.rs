fn main() {
    tonic_build::configure()
        .compile(&["proto/mayastor.proto"], &["proto"])
        .unwrap();
}
