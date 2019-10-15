use std::io::{self, Write};

enum Query {
    Help,
    Quit,
    Unrecognized,
}

fn parse_input(input: &str) -> Query {
    if input == String::from(".quit") {
        Query::Quit
    } else if input == String::from(".help") {
        Query::Help
    } else {
        Query::Unrecognized
    }
}

fn main() {
    println!("Starting minuSQLi (2019)");
    println!("Enter .help for usage hints");
    loop {
        print!(">> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();

        io::stdin().read_line(&mut input)
            .expect("Error reading line");

        let input: String = match input.trim().parse() {
            Ok(input) => input,
            Err(_) => continue,
        };

        let query = parse_input(&input);

        match query {
            Query::Help => {
                println!(".help   Show this message");
                println!(".quit   Exit this program");
            }
            Query::Quit => {
                println!("Shutting down.");
                return;
            },
            Query::Unrecognized => {
                println!("Unrecognized input");
            },
        }
    }
}
