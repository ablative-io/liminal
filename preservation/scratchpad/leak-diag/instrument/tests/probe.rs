#[test]
fn deliberate_leaker() {
    // child inherits our stderr and outlives the test process
    std::process::Command::new("sleep").arg("5").spawn().expect("spawn");
}
#[test]
fn clean_control() { assert_eq!(1 + 1, 2); }
