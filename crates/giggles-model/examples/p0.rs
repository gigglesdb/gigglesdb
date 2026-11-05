fn main() {
    for h in giggles_model::fixtures::golden_histories() {
        giggles_model::checker::verify(h.domain, &h.events, &h.final_state)
            .expect("golden history verifies");
        println!("== {} ==\n{}", h.name, h.transcript.trim_end());
        if std::env::args().any(|a| a == "--histories") {
            println!("{:#?}", h);
        }
    }
}
