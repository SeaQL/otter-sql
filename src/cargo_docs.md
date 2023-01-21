**OtterSQL** is an embedded SQL executor made for use in tests.

See [README](https://github.com/SeaQL/otter-sql#readme) for more details.

This crate documentation describes the external API of OtterSQL. See above if you only want to
use the CLI.

## Use as a library

### Installation

Add the following to your `Cargo.toml`

```toml
otter-sql = "0.1"
```

### Getting started

```rust
use otter_sql::VirtualMachine;

fn main() {
    // initialize an OtterSQL VM
    let mut vm = VirtualMachine::default();

    // execute SQL!
    vm.execute(
        "
        CREATE TABLE table1
        (
            col1 INTEGER PRIMARY KEY NOT NULL,
            col2 STRING NOT NULL
        )
        ",
    ).unwrap();

    // insert some values
    vm.execute(
        "
        INSERT INTO table1 VALUES
            (2, 'bar'),
            (3, 'aaa')
        ",
    ).unwrap();

    // two unwraps because this returns a `Result<Option<Table>, ...>`.
    let res = vm.execute("SELECT * FROM table1").unwrap().unwrap();

    println!("{}", res);
}
```

Output:
```text
╭──────┬──────╮
│ col1 │ col2 │
├──────┼──────┤
│ 2    │ bar  │
│ 3    │ aaa  │
╰──────┴──────╯
```
